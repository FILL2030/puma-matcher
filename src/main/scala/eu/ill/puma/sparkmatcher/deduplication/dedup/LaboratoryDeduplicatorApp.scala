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

import eu.ill.puma.sparkmatcher.deduplication.LaboratoryDeduplicationStage
import eu.ill.puma.sparkmatcher.utils.conf.{ProgramConfig, Spark}
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.log4j.{Level}

object LaboratoryDeduplicatorApp {

  def main(args: Array[String]) {
    LaboratoryDeduplicatorApp.run()
  }

  def run() = {
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = new Spark("Person Deduplicator")

    Logger.info("LaboratoriesDedup", "no config", s"running on database : ${ProgramConfig.matchingDatabaseUrl}")

    // Get persons DataFrame
    val query = "(" +
      "select l.id, l.name, l.short_name, l.country from laboratory l" +
      " where l.obsolete = false" +
      //			" limit 20000" +
      ") laboratory_limited"

    // Read from database
    val laboratiesDataFrame = spark.dataFrameFromDatabase(ProgramConfig.corpusDatabaseUrl, query)

    // deduplicate laboratories
    val deduplicatedLaboratories = LaboratoryDeduplicationStage.deduplicateLaboratories(laboratiesDataFrame, spark).cache()

    //Write to database
    LaboratoryDeduplicationStage.writeToDatabase(deduplicatedLaboratories)

    //log
    Logger.info("LaboratoriesDedup", "no config", s"Deduplicated  ${deduplicatedLaboratories.count()} laboratories")
  }
}