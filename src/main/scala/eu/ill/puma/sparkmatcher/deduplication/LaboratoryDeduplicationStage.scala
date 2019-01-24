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
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql._

object LaboratoryDeduplicationStage {

	def deduplicateLaboratories(laboratories: DataFrame, spark: Spark): Dataset[(Long, Long)] = {

		// !!! IMPORTANT to import this !!!
		import spark.session.implicits._

		// repartition by Id improves perf by ~x3
		val partitionedLaboratories = laboratories.repartition($"id")

		// Clean the laboratory names
		val cleanedLaboratories = LaboratoryCleanerStage.cleanLaboratories(partitionedLaboratories, spark)

		val laboratories1 = cleanedLaboratories.toDF(Seq("id1", "name1", "short_name1", "country1"): _*)
		val laboratories2 = cleanedLaboratories.toDF(Seq("id2", "name2", "short_name2", "country2"): _*)

		// Try to limit join by using conditions for limiting the join
		laboratories1.createOrReplaceTempView("laboratories1")
		laboratories2.createOrReplaceTempView("laboratories2")
		val joinedLaboratories = spark.session.sqlContext.sql(
			"select l1.*, l2.* from laboratories1 l1, laboratories2 l2" +
				" where l1.id1 > l2.id2" + // remove half of all matches: avoid duplicating the same match (avoid 6 -> 12, but keep 12 -> 6)
				" and l1.name1 != ''" +
				" and l1.name1 is not null" +
				" and l2.name2 != ''" +
				" and l2.name2 is not null" +
				" and l1.name1 == l2.name2" + // areEqual(nameA, nameB)
//				" and (l1.short_name1 == l2.short_name2 or (l1.short_name1 is null and l2.short_name2 is null))" + // areEqualOrNull(shortNameA, shortNameB)
				" and (l1.country1 == l2.country2 or (l1.country1 is null and l2.country2 is null))" // areEqualOrNull(countryA, countryB)
		)

		// Map tables onto tuples of ids
		val mappedLaboratoryIds = joinedLaboratories.map(row => (row.getAs[Long]("id1"), row.getAs[Long]("id2")))
		mappedLaboratoryIds.cache()

		// Data reduction : Find all linked pairs and reduce to lowest lookup value
		val laboratoryIdNodes = mappedLaboratoryIds.flatMap(tuple => List((tuple._1, "node"), (tuple._2, "node"))).distinct()
		val laboratoryIdEdges = mappedLaboratoryIds.map(tuple => Edge(tuple._1, tuple._2, "edge"))

		// Build the initial Graph
		val graph = Graph(laboratoryIdNodes.rdd, laboratoryIdEdges.rdd)

		// Do everything and remove elements with equal keys and values
		val cleanedLookupTable = graph.connectedComponents().vertices.filter(vertex => vertex._1 != vertex._2)

		cleanedLookupTable.toDS()
	}

	def writeToDatabase(lookupTable: Dataset[(Long, Long)]) = {
		lookupTable.toDF("lookup_id", "laboratory_id").write.mode(SaveMode.Append).jdbc(ProgramConfig.matchingDatabaseUrl, "laboratory_deduplication_lookup", ProgramConfig.dbProperties)
	}

}
