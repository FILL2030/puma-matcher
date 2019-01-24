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
package eu.ill.puma.sparkmatcher.matching.stage

import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

class MatchCandidatePersisterStage(override val input: List[String],
                                   override val output: String,
                                   val tableName: String,
                                   val typeDataSource: DataSource,
                                   val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                                   val dbProperties: Properties = ProgramConfig.dbProperties) extends Stage(input, output) {
  override def run(config: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {

    input.foreach(row => this.persist(row._2, typeDataSource, tableName))

    None
  }

  def persist(dataFrame: DataFrame, typeDataSource: DataSource, tableName: String) = {
    import dataFrame.sparkSession.implicits._

    val idUdf = udf[Int, String](x => EntityType.getByValue(x).id)

    //prepare type data
    val documentTypeDF = typeDataSource.loadData._2
    val doc1Type = documentTypeDF.withColumnRenamed("document_version_id", "document_version1_id").withColumnRenamed("document_type", "document1_type")
    val doc2Type = documentTypeDF.withColumnRenamed("document_version_id", "document_version2_id").withColumnRenamed("document_type", "document2_type")

    //prepare MC
    val matchCandidateToPersist = dataFrame.join(doc1Type, Seq("document_version1_id")).join(doc2Type, Seq("document_version2_id"))
      .withColumn("pair_id", when($"document_version1_id" < $"document_version2_id", $"document_version1_id" * 1000000 + $"document_version2_id").otherwise($"document_version2_id" * 1000000 + $"document_version1_id"))
      .groupBy($"pair_id", $"score_type")
      .agg(
        first($"document_version2_id") as "document_version2_id",
        first($"document_version1_id") as "document_version1_id",
        first($"id") as "id",
        sum($"item_count") as "item_count",
        first($"score") as "score",
        first($"document1_type") as "document1_type",
        first($"document2_type") as "document2_type"
      )
      .cache()

    Logger.info("MatchCandidatePersisterStage", "debug", s"Match number to save :")
    matchCandidateToPersist.groupBy($"score_type").count().show(100)

    //save data
    matchCandidateToPersist.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, tableName, dbProperties)
  }


  override def name = MatchCandidatePersisterStageName

  override def produceData(config: PipelineConfig): Boolean = false

  override def acceptAnyInput = false

  override def validInputType(config: PipelineConfig): List[DataFrameType] = MatchCandidateDfType :: Nil

  override def isOptional(config: PipelineConfig): Boolean = false

  override def maxInputNumber(config: PipelineConfig): Int = Integer.MAX_VALUE
}
