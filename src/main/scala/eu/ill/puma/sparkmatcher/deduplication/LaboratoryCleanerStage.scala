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

import eu.ill.puma.sparkmatcher.utils.conf.Spark
import eu.ill.puma.sparkmatcher.utils.StringComparer.{isEmail, nullOrEmpty}
import eu.ill.puma.sparkmatcher.utils.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

object LaboratoryCleanerStage {


	def cleanLaboratories(persons: DataFrame, spark: Spark): DataFrame = {

		val nameCleaner = (value:String) => {
			var cleanName: String = value
			if (!nullOrEmpty(cleanName)) {
				cleanName = cleanName.trim
				if (cleanName.contains("-")) {
					cleanName = StringUtils.rejoin(cleanName, "-", " ")
				}

				cleanName = cleanName.replaceAll("institute", "institut")
				cleanName = cleanName.replaceAll("instituut", "institut")
				cleanName = cleanName.replaceAll("istituto", "institut")

			} else if (value == "") {
				cleanName = null
			}

			cleanName
		}

		val nameCleanerUDF = udf(nameCleaner)

		val cleanedPersons = persons
			.withColumn("name", nameCleanerUDF(col("name")))

		cleanedPersons
	}

}
