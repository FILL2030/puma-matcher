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

class DocumentVersionIdFilter(validIdDataSource: DataSource) extends Filter {
  override def run(input: DataFrame): DataFrame = {

    import input.sparkSession.implicits._

    val validIdDF = validIdDataSource.loadData._2

    val filteredMatchCandidate = input
      .join(validIdDF, $"document_version1_id" === $"document_version_id")
      .drop($"document_version_id")
      .join(validIdDF, $"document_version2_id" === $"document_version_id")
      .drop($"document_version_id")

    filteredMatchCandidate
  }

  override def name: String = "document version id filter"
}
