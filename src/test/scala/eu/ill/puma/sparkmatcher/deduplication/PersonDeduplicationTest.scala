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
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class PersonDeduplicationTest extends FlatSpec with BeforeAndAfter with Matchers {

	var opts: Map[String, String] = null
	Logger.getLogger("org").setLevel(Level.ERROR)

	before {
		this.opts = Map(
			"url" -> ProgramConfig.corpusDatabaseUrl,
			"user" -> ProgramConfig.username,
			"password" -> ProgramConfig.password,
			"driver" -> ProgramConfig.driver,
			"spark.sql.crossJoin.enabled" -> "true",
			"numPartitions" -> "20")
	}

	it should "deduplicate persons" in {
		// Get persons DataFrame
		val query = "(" +
			"select p.id, p.firstname, p.lastname, p.email, p.publication_name, p.orcid_id, p.researcher_id from person p" +
			" where p.obsolete = false" +
			" and p.id in (28, 119708, 121169, 223155, 2003, 276803, 253029, 300001)" +
			" limit 5000" +
			") person_limited"

		this.opts += ("dbtable" -> query)

		val spark = new Spark("PersonCleanerTest")
		val personsDataFrame = spark.session.
			read.
			format("jdbc").
			options(this.opts).
			load()
		personsDataFrame.show(10)

		// Clean persons first
		val cleanedPersons = PersonCleanerStage.cleanPersons(personsDataFrame, spark)

		cleanedPersons.show(10)

		// deduplicate persons
		val deduplicatedPersons = PersonDeduplicationStage.deduplicatePersons(cleanedPersons, spark)
		deduplicatedPersons.cache()
		val deduplicatedPersonsList = deduplicatedPersons.collect()

		deduplicatedPersons.show(10)

		println("Deduplicated " + deduplicatedPersons.count() + " persons")
	}
}
