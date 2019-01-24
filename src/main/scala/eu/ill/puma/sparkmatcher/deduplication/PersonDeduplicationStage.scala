/*
 * Copyright 2019 Institut Laue–Langevin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.ill.puma.sparkmatcher.deduplication

import eu.ill.puma.sparkmatcher.utils.conf.{ProgramConfig, Spark}
import eu.ill.puma.sparkmatcher.deduplication.domain.Person
import eu.ill.puma.sparkmatcher.utils.StringComparer._
import eu.ill.puma.sparkmatcher.utils.StringUtils._
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql._

object PersonDeduplicationStage {

	def deduplicatePersons(persons: DataFrame, spark: Spark): Dataset[(Long, Long)] = {

		val publisherNameProbablyEqualToNames = (personA: Person, personB: Person) => {
			val publicationName = personB.publication_name
			val firstName = personA.firstname
			val lastName = firstWord(personA.lastname)

			// Verify that lastname and publisher name are not null
			if (!areAnyNullOrEmpty(lastName, firstName, publicationName)) {
				if (publicationName.startsWith(lastName + " " + firstName.substring(0, 1))) {
					// Simplest test: verify that publication name has last name + first initial
					true

				} else if ((publicationName.startsWith(lastName + " ") && publicationName.endsWith(" " + firstName))
					|| (publicationName.endsWith(" " + lastName) && publicationName.startsWith(firstName + " "))) {
					// Publisher name starts or end with last name/first name (with a separator)
					true

				} else {
					false
				}

			} else {
				false
			}
		}

		val getNamesProbablyEqual: (Person, Person) => Boolean = (personA, personB) => {
			if (publisherNameProbablyEqualToNames(personA, personB) || publisherNameProbablyEqualToNames(personB, personA)) {
				true

			} else if (areEqual(firstWord(personA.lastname), firstWord(personB.lastname))) {
				// Compare equality of last names

				if (areAnyNullOrEmpty(personA.firstname, personB.firstname)) {
					// If either first names are null then not ok
					false

				} else if (!nullOrShorterThan(personA.firstname, 2) && !nullOrShorterThan(personB.firstname, 2) && areEqual(personA.firstname, personB.firstname)) {
					// If both first names greater than two chars and equal then ok
					true

				} else if (areNotNullAndNotEmptyAndFirstLetterEqual(personA.firstname, personB.firstname)) {
					// If are null or first letters match then ok
					true

				} else {
					false
				}

			} else {
				false
			}
		}

		val arePersonsIdentical: ((Person, Person)) => Boolean = (personPair) => {

			val personA = personPair._1
			val personB = personPair._2

			// Check if names are probably the same using first, last and publisher names
			val namesProbablyEqual = getNamesProbablyEqual(personA, personB)

			namesProbablyEqual

//			if (areEqual(personA.orcid_id, personB.orcid_id) && namesProbablyEqual) {
//				// If orchid id the same and names the same then it is a duplicate
//				true
//
//			} else if (areEqual(personA.researcher_id, personB.researcher_id) && namesProbablyEqual) {
//				// If researcher id the same and names the same then it is a duplicate
//				true
//
//			} else if (areEqual(personA.email, personB.email) && namesProbablyEqual) {
//				// If email the same and names the same then it is a duplicate
//				true
//
//			} else if ((areAllNullOrEmpty(personA.orcid_id, personA.researcher_id, personA.email) || areAllNullOrEmpty(personB.orcid_id, personB.researcher_id, personB.email)) && namesProbablyEqual) {
//				// If all truly comparable values are empty/null then just check similarity of names
//				true
//
//			} else {
//				false
//			}
		}

		// !!! IMPORTANT to import this !!!
		import spark.session.implicits._

		// repartition by Id improves perf by ~x3
		val partitionedPersons = persons.repartition( $"id")

		// Clean persons first - normalises lastname and publication name
		val cleanedPersons = PersonCleanerStage.cleanPersons(partitionedPersons, spark)

		val persons1 = cleanedPersons.toDF(Seq("id1", "firstname1", "lastname1", "email1", "publication_name1", "orcid_id1", "researcher_id1", "first_lastname1"): _*)
		val persons2 = cleanedPersons.toDF(Seq("id2", "firstname2", "lastname2", "email2", "publication_name2", "orcid_id2", "researcher_id2", "first_lastname2"): _*)

		// Cross join performs full cartesian join
		//val joinedPersons = persons1.crossJoin(persons2)

		// Try to limit join by using conditions for limiting the join
		persons1.createOrReplaceTempView("persons1")
		persons2.createOrReplaceTempView("persons2")
		val joinedPersons = spark.session.sqlContext.sql(
			"select p1.*, p2.* from persons1 p1, persons2 p2" +
				" where p1.id1 > p2.id2" + // remove half of all matches: avoid duplicating the same match (avoid 6 -> 12, but keep 12 -> 6)
				" and p1.first_lastname1 == p2.first_lastname2" // requires PersonCleanerStage to run before
		)

		// Map tables onto tuples of persons
		val mappedPersons = joinedPersons.map { r: Row =>
			(Person(r.getAs("id1"), r.getAs("firstname1"), r.getAs("lastname1"), r.getAs("first_lastname1"), r.getAs("publication_name1"), r.getAs("email1"), r.getAs("orcid_id1"), r.getAs("researcher_id1")),
				Person(r.getAs("id2"), r.getAs("firstname2"), r.getAs("lastname2"), r.getAs("first_lastname2"), r.getAs("publication_name2"), r.getAs("email2"), r.getAs("orcid_id2"), r.getAs("researcher_id2")))
		}

		// Filter all the identical pairs
		val filteredPerson = mappedPersons.filter(arePersonsIdentical)

		val filteredPersonIds = filteredPerson.map(personTuple => (personTuple._1.id, personTuple._2.id))
		filteredPersonIds.cache()

		// Data reduction : Find all linked pairs and reduce to lowest lookup value
		val personIdNodes = filteredPersonIds.flatMap(tuple => List((tuple._1, "node"), (tuple._2, "node"))).distinct()
		val personIdEdges = filteredPersonIds.map(tuple => Edge(tuple._1, tuple._2, "edge"))

		// Build the initial Graph
		val graph = Graph(personIdNodes.rdd, personIdEdges.rdd)

		// Do everything and remove elements with equal keys and values
		val cleanedLookupTable = graph.connectedComponents().vertices.filter(vertex => vertex._1 != vertex._2)

		cleanedLookupTable.toDS()
	}

	def writeToDatabase(lookupTable: Dataset[(Long, Long)]) = {
		lookupTable.toDF("lookup_id", "person_id").write.mode(SaveMode.Append).jdbc(ProgramConfig.matchingDatabaseUrl, "person_deduplication_lookup", ProgramConfig.dbProperties)
	}

}
