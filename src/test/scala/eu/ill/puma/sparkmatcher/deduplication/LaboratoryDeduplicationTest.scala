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

class LaboratoryDeduplicationTest extends FlatSpec with BeforeAndAfter with Matchers {

	var opts: Map[String, String] = null

	before {
		this.opts = Map(
			"url" -> ProgramConfig.corpusDatabaseUrl,
			"user" -> ProgramConfig.username,
			"password" -> ProgramConfig.password,
			"driver" -> ProgramConfig.driver,
			"spark.sql.crossJoin.enabled" -> "true",
			"numPartitions" -> "20")
	}

	it should "deduplicate laboratories" in {
		// Get laboratories DataFrame
		val query = "(" +
			"select l.id, l.name, l.short_name, l.country from laboratory l" +
			" where l.obsolete = false" +
			" limit 5000" +
			") laboratory_limited"

		this.opts += ("dbtable" -> query)

		val spark = new Spark("LaboratoryDeduplicationTest")
		val laboratoriesDataFrame = spark.session.
			read.
			format("jdbc").
			options(this.opts).
			load()
		laboratoriesDataFrame.show(10)

		// deduplicate laboratories
		val deduplicatedLaboratories = LaboratoryDeduplicationStage.deduplicateLaboratories(laboratoriesDataFrame, spark)
		deduplicatedLaboratories.cache()
		val deduplicatedLaboratoriesList = deduplicatedLaboratories.collect()

		deduplicatedLaboratories.show(10)

		println("Deduplicated " + deduplicatedLaboratories.count() + " laboratories")
	}
}
