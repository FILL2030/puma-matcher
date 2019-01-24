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
import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, MatchCandidateDfType, PipelineConfig, TrainingStageName}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

class TrainingDataExtractionStage(
                                   override val input: List[String],
                                   override val output: String,
                                   val trainingIdsDataSource: DataSource,
                                   val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                                   val dbProperties: Properties = ProgramConfig.dbProperties) extends Stage(input, output) {

  override def run(config: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {

    //input
    val trainingIds = broadcast(trainingIdsDataSource.loadData._2.repartition(1))

    //extraction
    input.foreach(data => {
      this.extractTrainingData(trainingIds, data._2)
    })

    None
  }


  def extractTrainingData(trainingIds: DataFrame, dataFrame: DataFrame) = {

    import dataFrame.sparkSession.implicits._

    //compute
    val trainingData = dataFrame.join(trainingIds, $"document_version1_id" === $"training_id" || $"document_version2_id" === $"training_id")
      .withColumn("other_document_version_id", (($"document_version1_id" * $"document_version2_id") / $"training_id").cast("Int")).drop("document_version1_id", "document_version2_id")
      .withColumnRenamed("id", "match_id")

    //save
    trainingData.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "training_match_candidate", dbProperties)

  }

  override def produceData(config: PipelineConfig): Boolean = false

  override def acceptAnyInput = false

  override def validInputType(config: PipelineConfig): List[DataFrameType] = MatchCandidateDfType :: Nil

  override def name = TrainingStageName

  override def isOptional(config: PipelineConfig): Boolean = false

  override def maxInputNumber(config: PipelineConfig): Int = Integer.MAX_VALUE
}
