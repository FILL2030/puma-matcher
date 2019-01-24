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
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class PersonCleanerTest extends FlatSpec with BeforeAndAfter with Matchers {

	var opts: Map[String, String] = null

	before {
		this.opts = Map(
			"url" -> ProgramConfig.corpusDatabaseUrl,
			"user" -> ProgramConfig.username,
			"password" -> ProgramConfig.password,
			"driver" -> ProgramConfig.driver,
			"numPartitions" -> "20")
	}

	it should "clean the emails" in {
		// Get persons DataFrame
		val query = "(" +
			"select * from person" +
			" where email like '%<%'" +
			" order by id" +
			") person_limited"

		this.opts += ("dbtable" -> query)

		val spark = new Spark("PersonCleanerTest")
		val personsDataFrame = spark.session.
			read.
			format("jdbc").
			options(this.opts).
			load()

		personsDataFrame.show(10)

		val cleanedPersons = PersonCleanerStage.cleanPersons(personsDataFrame, spark)

		cleanedPersons.explain()
		cleanedPersons.show(10)

		val testPerson = cleanedPersons.rdd.take(1).last
		val email = testPerson.getAs[String]("email")

		email contains "<" should be (false)
	}


	it should "clean the first names" in {
		// Get persons DataFrame
		val query = "(" +
			"select * from person" +
			" where firstname like '%.%' or firstname like '%.'" +
			" order by id" +
			") person_limited"


		this.opts += ("dbtable" -> query)

		val spark = new Spark("PersonCleanerTest")
		val personsDataFrame = spark.session.
			read.
			format("jdbc").
			options(this.opts).
			load()

		personsDataFrame.show(10)

		val cleanedPersons = PersonCleanerStage.cleanPersons(personsDataFrame, spark)

		cleanedPersons.explain()
		cleanedPersons.show(10)

		val testPerson = cleanedPersons.rdd.take(1).last
		val firstname = testPerson.getAs[String]("firstname")

		firstname contains "." should be (false)
	}

	it should "clean the publication name" in {
		// Get persons DataFrame
		val query =
			"(" +
				"select * from person" +
				" where publication_name like '%.%' or publication_name like '%,%'" +
				" order by id" +
				") person_limited"

		this.opts += ("dbtable" -> query)

		val spark = new Spark("PersonCleanerTest")
		val personsDataFrame = spark.session.
			read.
			format("jdbc").
			options(this.opts).
			load()

		personsDataFrame.show(10)

		val cleanedPersons = PersonCleanerStage.cleanPersons(personsDataFrame, spark)

		cleanedPersons.explain()
		cleanedPersons.show(10)

		val testPerson = cleanedPersons.rdd.take(1).last
		val publicationName = testPerson.getAs[String]("publication_name")

		publicationName contains "." should be (false)
		publicationName contains "," should be (false)
	}


}
