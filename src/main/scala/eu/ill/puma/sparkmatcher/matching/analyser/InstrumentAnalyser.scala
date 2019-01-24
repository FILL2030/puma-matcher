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
package eu.ill.puma.sparkmatcher.matching.analyser

import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntitiesIdDfType, EntityType, TextDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

class InstrumentAnalyser(val instrumentDataSource: DataSource,
                         val instrumentNameDataSource: DataSource,
                         val typeDataSource: DataSource,
                         val tableName: String = "instrument",
                         val saveEntities: Boolean = true,
                         val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                         val dbProperties: Properties = ProgramConfig.dbProperties) extends Analyser {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //init
    val textDf = input.head._2
    val instrumentDf = instrumentDataSource.loadData._2
    val documentTypeDf = typeDataSource.loadData._2
    import textDf.sparkSession.implicits._

    //extract instrument name
    val instrumentNameDf = instrumentNameDataSource.loadData._2.select($"instrument_code" as "instrument_name" , $"instrument_id" as "entity_id").distinct()
    //    val instrumentNameDf = instrumentDf.select($"code" as "instrument_name", $"entity_id").distinct()

    /**
      * Publication
      */

    //extract words
    val words = textDf.join(documentTypeDf, Seq("document_version_id"))
      .filter($"document_type".startsWith("PROPOSAL") === false)
      .groupBy($"document_version_id", $"document_type")
      .agg(concat_ws(" ", collect_list($"text")) as "text")
      .select($"document_version_id", $"document_type", posexplode(split($"text", "\\W+")))
      .withColumnRenamed("pos", "word_position")
      .withColumnRenamed("col", "word")

    //extract instrument
    val publicationInstrument = words.join(instrumentNameDf, lower($"word") === lower($"instrument_name")).drop("word")
      .withColumnRenamed("word_position", "instrument_position")
      .withColumnRenamed("document_version_id", "instrument_dv_id")
      .drop("document_type")

    //Merge instrument with sentence
    val publicationInstrumentWithSentence = publicationInstrument.join(words, $"instrument_dv_id" === $"document_version_id" && $"word_position" <= $"instrument_position" + 10 && $"word_position" >= $"instrument_position" - 10)
      .groupBy($"document_version_id", $"document_type", $"instrument_position", $"instrument_name", $"entity_id")
      .agg(
        collect_list("word") as "instrument_sentence"
      )
      .drop("instrument_position")

    /**
      * Proposal
      */

    val proposalInstrument = instrumentDf
      .join(documentTypeDf, Seq("document_version_id"))
      .filter($"document_type".startsWith("PROPOSAL"))
      .select($"document_version_id", $"document_type", $"code" as "instrument_name", $"entity_id", lit(null) as "instrument_sentence")

    /**
      * Result
      */

    val result = proposalInstrument union publicationInstrumentWithSentence.cache()

    //save instrument
    if (saveEntities) {
      result.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, tableName, dbProperties)
    }

    //return
    (EntitiesIdDfType, result.select($"document_version_id", $"entity_id").distinct())
  }

  override def name: String = "Instrument analyser"

  override def maxInputNumber: Int = 1

  override def validInputType: List[DataFrameType] = TextDfType :: Nil


}
