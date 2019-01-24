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

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, PipelineConfig, ViewStageName}
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.DataFrame

class CountStage(override val input: List[String], override val output: String) extends Stage(input, output) {

  override def run(config: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {

    input.foreach(row => {
      Logger.info(this.name.stringValue, config.name, s"Count dataFrame of type(${input.last._1.stringValue}) : ${input.last._2.count()}")
    })

    None
  }

  override def name = ViewStageName

  override def produceData(config: PipelineConfig): Boolean = false

  override def acceptAnyInput = true

  override def validInputType(config: PipelineConfig): List[DataFrameType] = Nil

  override def isOptional(config: PipelineConfig): Boolean = false

  override def maxInputNumber(config: PipelineConfig): Int = Integer.MAX_VALUE
}
