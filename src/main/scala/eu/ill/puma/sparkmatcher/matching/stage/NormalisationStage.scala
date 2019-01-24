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

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, NormalisationStageName, PipelineConfig, StageName}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}

class NormalisationStage(override val input: List[String],
                         override val output: String,
                         val normalisationPercentile: Int = ProgramConfig.normalisationPercentile,
                         val normalisationValue: Int = ProgramConfig.normalisationValue) extends Stage(input, output) {

  override def run(config: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {

    if (config.matchEntityType.isEmpty) {
      Logger.error(this.name.stringValue, config.matchEntityType.get.stringValue, s"Invalid config, matchEntityType not present")
      throw new Exception(s"Invalid config for stage ${name.stringValue}")

    } else if (input.size != 1) {
      Logger.error(this.name.stringValue, config.matchEntityType.get.stringValue, s"Invalid input, ${input.size} provided instead of 1")
      throw new Exception(s"Invalid input for stage ${name.stringValue}")

    } else {
      Some(input.head._1, this.normalise(input.head._2, config))
    }
  }

  def normalise(scoreDataFrame: DataFrame, config: PipelineConfig): DataFrame = {

    import scoreDataFrame.sparkSession.implicits._

    val count = scoreDataFrame.count()

    //enough data to compute normalisation
    if (count > 100) {
      val percentileRank = ((scoreDataFrame.count() * (100 - normalisationPercentile)) / 100).toInt

      val percentileScore = scoreDataFrame.withColumn("rank", row_number().over(Window.orderBy(col("score").desc))).filter($"rank" === lit(percentileRank)).select("score").first().getDouble(0)

      val scoreNormalisationFactor = normalisationValue / percentileScore

      val (normalizedScore, normalisationFactor) = (scoreDataFrame.withColumn("normalized_" + "score", col("score") * scoreNormalisationFactor).drop("score").withColumnRenamed("normalized_" + "score", "score"), scoreNormalisationFactor)

      DbManager.saveMatchInfo(config.matchEntityType.get.stringValue + "_normalisation_factor", normalisationFactor.toString)

      normalizedScore
    } else {
      val (normalizedScore, normalisationFactor) = (scoreDataFrame, 1.0)

      DbManager.saveMatchInfo(config.matchEntityType.get.stringValue + "_normalisation_factor", normalisationFactor.toString)

      normalizedScore
    }
  }

  override def name: StageName = NormalisationStageName

  override def acceptAnyInput = true

  override def validInputType(config: PipelineConfig): List[DataFrameType] = Nil

  override def produceData(config: PipelineConfig): Boolean = true

  override def isOptional(config: PipelineConfig): Boolean = false

  override def maxInputNumber(config: PipelineConfig): Int = 1
}
