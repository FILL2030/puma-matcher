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

import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.DataFrame

class AnalyserStage(override val input: List[String], override val output: String) extends Stage(input, output) {

  override def run(config: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {
    if (config.analyser.isEmpty) {
      None

    } else if (config.matchEntityType.isEmpty) {
      Logger.error(this.name.stringValue, config.name, s"Invalid config, matchEntityType not present")
      throw new Exception(s"Invalid config for stage ${name.stringValue}")

    } else {
      Some(config.analyser.get.run(input, config.matchEntityType.get))

    }
  }

  override def maxInputNumber(config: PipelineConfig): Int = config.analyser.get.maxInputNumber

  override def name: StageName = AnalysisStageName

  override def acceptAnyInput = false

  override def validInputType(config: PipelineConfig): List[DataFrameType] = config.analyser.get.validInputType

  override def produceData(config: PipelineConfig): Boolean = config.analyser.nonEmpty

  override def isOptional(config: PipelineConfig): Boolean = config.analyser.isEmpty
}
