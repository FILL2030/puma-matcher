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
package eu.ill.puma.sparkmatcher.matching.filter

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import org.apache.spark.sql.DataFrame


class InstrumentTechniqueFilter(instrumentTechniqueDataSource: DataSource) extends Filter {

  /**
    * run this filter which keep only the technics which correspond to instruments find in the documents
    *
    * @param input
    * @return
    */
  override def run(input: DataFrame): DataFrame = {

    import input.sparkSession.implicits._

    val possibleTechniqueByDocument = instrumentTechniqueDataSource.loadData._2
      .select($"scientific_technique_id" as "tag_id", $"document_version_id")
      .distinct()

    val filteredTechnic = possibleTechniqueByDocument.join(input, Seq("tag_id", "document_version_id"))

    filteredTechnic
  }

  override def name: String = "instrument technique filter"
}
