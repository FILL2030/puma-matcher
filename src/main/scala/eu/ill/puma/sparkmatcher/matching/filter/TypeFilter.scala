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
import org.apache.spark.sql.functions._

class TypeFilter(typeDataSource: DataSource) extends Filter {
  override def run(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._

    val documentTypeDF = broadcast(typeDataSource.loadData._2)

    val doc1Type = documentTypeDF
      .withColumnRenamed("document_version_id", "document_version1_id")
      .withColumnRenamed("document_type", "document1_type")

    val doc2Type = documentTypeDF
      .withColumnRenamed("document_version_id", "document_version2_id")
      .withColumnRenamed("document_type", "document2_type")

    val matchCandidateWithType = input.join(doc1Type, Seq("document_version1_id")).join(doc2Type, Seq("document_version2_id"))

    //filter Proposal / matches
    var filteredMatchCandidate = matchCandidateWithType.filter(($"document1_type".startsWith("PROPOSAL") and !$"document2_type".startsWith("PROPOSAL"))
      or ($"document2_type".startsWith("PROPOSAL") and !$"document1_type".startsWith("PROPOSAL")))

    filteredMatchCandidate.drop("document1_type", "document2_type")
  }

  override def name: String = "Type filter"
}
