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

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntityType}
import org.apache.spark.sql.DataFrame

abstract class Analyser extends Serializable {
  def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame)

  def name: String

  def maxInputNumber: Int

  def validInputType: List[DataFrameType]
}
