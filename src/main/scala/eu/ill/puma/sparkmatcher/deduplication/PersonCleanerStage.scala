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

import java.text.Normalizer

import eu.ill.puma.sparkmatcher.utils.conf.Spark
import eu.ill.puma.sparkmatcher.utils.StringComparer.{isEmail, nullOrEmpty, areEqualOrNull}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import eu.ill.puma.sparkmatcher.utils.StringUtils._

object PersonCleanerStage {


	def cleanPersons(persons: DataFrame, spark: Spark): DataFrame = {

		val cleanedEmailsCounter = spark.accumulator("cleanedEmailsCounter")
		val cleanedFirstNameCounter = spark.accumulator("cleanedFirstNameCounter")
		val cleanedPublicationNameCounter = spark.accumulator("cleanedPublicationNameCounter")

		val normalizeText = (value: String) => {
			if (value == null) {
				null
			} else {
				var normalizedText: String = value

				if (normalizedText.contains("ä")) {
					normalizedText = rejoin(normalizedText, "ä", "ae")
				}
				if (normalizedText.contains("ö")) {
					normalizedText = rejoin(normalizedText, "ö", "oe")
				}
				if (normalizedText.contains("ü")) {
					normalizedText = rejoin(normalizedText, "ü", "ue")
				}
				if (normalizedText.contains("a¨")) {
					normalizedText = rejoin(normalizedText, "¨", "e")
				}
				if (normalizedText.contains("o¨")) {
					normalizedText = rejoin(normalizedText, "¨", "e")
				}
				if (normalizedText.contains("u¨")) {
					normalizedText = rejoin(normalizedText, "¨", "e")
				}
				if (normalizedText.contains("oe")) {
					normalizedText = rejoin(normalizedText, "oe", "o")
				}

				normalizedText = Normalizer.normalize(normalizedText, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
				normalizedText
			}
		}

		val emailCleaner = (value: String) => {
			var cleanEmail: String = value
			var isCleaned = false
			if (!nullOrEmpty(cleanEmail)) {
				// Clean email
				cleanEmail = cleanEmail.trim()
				if (cleanEmail.contains("<") && cleanEmail.contains(">")) {
					cleanEmail = cleanEmail.substring(cleanEmail.indexOf("<") + 1, cleanEmail.indexOf(">"))
					isCleaned = true
				}

				// Remove bad emails
				if (!isEmail(cleanEmail)) {
					cleanEmail = null
					isCleaned = true
				}
			} else if (cleanEmail == "") {
				cleanEmail = null
			}

			if (isCleaned) {
				cleanedEmailsCounter.add(1)
			}

			cleanEmail
		}

		val emptyValueCleaner = (value: String) => {
			if (value == "") {
				null
			} else {
				value
			}
		}

		val firstNameCleaner = (value: String) => {
			var cleanName: String = normalizeText(value)
			var cleaned: Boolean = false

			if (!nullOrEmpty(cleanName)) {
				cleanName = cleanName.trim
				if (cleanName.contains(".")) {
					cleanName = rejoin(cleanName, ".", " ")
				}
				if (cleanName.contains("-")) {
					cleanName = rejoin(cleanName, "-", " ")
				}
			}

			cleaned = !areEqualOrNull(cleanName, value)
			if (cleaned) {
				cleanedFirstNameCounter.add(1)
			}
			cleanName
		}

		val publicationNameCleaner = (value: String) => {
			var cleanPublicationName: String = normalizeText(value)
			var cleaned: Boolean = false
			if (!nullOrEmpty(cleanPublicationName)) {
				if (cleanPublicationName.length < 4) {
					cleanPublicationName = null

				} else {
					if (cleanPublicationName.contains(".")) {
						cleanPublicationName = rejoin(cleanPublicationName, ".", " ")
					}
					if (cleanPublicationName.contains(",")) {
						cleanPublicationName = rejoin(cleanPublicationName, ",", " ")
					}
				}
			}

			cleaned = !areEqualOrNull(cleanPublicationName, value)

			if (cleaned) {
				cleanedPublicationNameCounter.add(1)
			}

			cleanPublicationName
		}

		val lastAndPublicationNameNormalizer = (lastName: String, publicationName: String) => {
			if (!nullOrEmpty(lastName)) {
				lastName

			} else if (!nullOrEmpty(publicationName)) {
				if (publicationName.contains(",")) {
					publicationName.substring(0, publicationName.indexOf(","))

				} else if (publicationName.contains(" ")) {
					publicationName.substring(0, publicationName.indexOf(" "))

				} else {
					publicationName
				}

			} else {
				null
			}
		}

		val nameNormalizer = (lastName: String, publicationName: String) => {
			val normalizedLastAndPublicationName = lastAndPublicationNameNormalizer(lastName, publicationName)

			// Remove any hyphens
			var cleanName: String = normalizedLastAndPublicationName
			if (cleanName.contains("-")) {
				cleanName = rejoin(cleanName, "-", " ")
			}

			cleanName =normalizeText(cleanName)

			cleanName
		}

		val firstLastNameCreator = (lastName: String, publicationName: String) => {
			var firstLastName = normalizeText(firstWord(lastName))

			if (firstLastName == null) {
				firstLastName = firstWord(publicationName)
			}

			firstLastName
		}

		val emptyValueCleanerUDF = udf(emptyValueCleaner)
		val emailCleanerUDF = udf(emailCleaner)
		val firstNameCleanerUDF = udf(firstNameCleaner)
		val publicationNameCleanerUDF = udf(publicationNameCleaner)
		val nameNormalizerUDF = udf(nameNormalizer)
		val firstLastNameCreatorUDF = udf(firstLastNameCreator)

		val cleanedPersons = persons
			.withColumn("orcid_id", emptyValueCleanerUDF(col("orcid_id")))
			.withColumn("researcher_id", emptyValueCleanerUDF(col("researcher_id")))
			.withColumn("email", emailCleanerUDF(col("email")))
			.withColumn("firstname", firstNameCleanerUDF(col("firstname")))
			.withColumn("lastname", nameNormalizerUDF(col("lastname"), col("publication_name")))
			.withColumn("publication_name", publicationNameCleanerUDF(col("publication_name")))
			.withColumn("first_lastname", firstLastNameCreatorUDF(col("lastname"), col("publication_name")))

		cleanedPersons
	}

}
