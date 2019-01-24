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
package eu.ill.puma.sparkmatcher.deduplication.dedup

import eu.ill.puma.sparkmatcher.deduplication.PersonDeduplicationStage
import eu.ill.puma.sparkmatcher.utils.conf.{ProgramConfig, Spark}
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.log4j.Level

object PersonDeduplicatorApp {

  def main(args: Array[String]) {
    PersonDeduplicatorApp.run
  }

  def run = {
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = new Spark("Person Deduplicator")

    Logger.info("PersonDedup", "no config", s"running on database : ${ProgramConfig.matchingDatabaseUrl}")

    // Reset person deduplication DB
    DbManager.resetPersonDeduplicationLookup()

    // Get persons DataFrame
    val query = "(" +
      "select p.id, p.firstname, p.lastname, p.email, p.publication_name, p.orcid_id, p.researcher_id from person p" +
      " where p.obsolete = false" +
      //			" limit 200000" +
      ") person_limited"

    // Read from database
    val personsDataFrame = spark.dataFrameFromDatabase(ProgramConfig.corpusDatabaseUrl, query)

    // deduplicate persons
    val deduplicatedPersons = PersonDeduplicationStage.deduplicatePersons(personsDataFrame, spark).cache()

    //write to database
    PersonDeduplicationStage.writeToDatabase(deduplicatedPersons)

    //log
    Logger.info("PersonDedup", "no config", s"Deduplicated  ${deduplicatedPersons.count()} persons")

  }
}