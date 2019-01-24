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

import java.sql.Timestamp

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.{DataFrame, Row}

class DateFilter(dateDataSource: DataSource,
                 documentVersionSourceDataSource: DataSource) extends Filter {

  override def run(input: DataFrame): DataFrame = {

    val dateDataFrame = dateDataSource.loadData._2.na.fill(Map("date" -> "1900-01-01 00:00:00"))
    val sourceDataFrame = documentVersionSourceDataSource.loadData._2

    import dateDataFrame.sparkSession.implicits._

    val doc1Data = dateDataFrame.withColumnRenamed("document_version_id", "document_version1_id").withColumnRenamed("document_type", "document1_type").withColumnRenamed("date", "document_version1_date")
    val doc2Data = dateDataFrame.withColumnRenamed("document_version_id", "document_version2_id").withColumnRenamed("document_type", "document2_type").withColumnRenamed("date", "document_version2_date")

    val doc1Source = sourceDataFrame.withColumnRenamed("document_version_id", "document_version1_id").withColumnRenamed("entity", "doc1_source")
    val doc2Source = sourceDataFrame.withColumnRenamed("document_version_id", "document_version2_id").withColumnRenamed("entity", "doc2_source")


    val matchCandidateWithFilterData = input
      .join(doc1Data, Seq("document_version1_id"))
      .join(doc2Data, Seq("document_version2_id"))
      .join(doc1Source, Seq("document_version1_id"))
      .join(doc2Source, Seq("document_version2_id"))
      .repartition($"document_version1_id", $"document_version2_id")

    val result = matchCandidateWithFilterData.filter(dateFilterFunction).drop("document_version1_date", "document_version2_date", "document1_type", "document2_type", "doc1_source", "doc2_source")

    result
  }

  def dateFilterFunction = new FilterFunction[Row]() {
    override def call(row: Row): Boolean = {

      val doc1Type = row.getAs[String]("document1_type")
      val doc2Type = row.getAs[String]("document2_type")
      val doc1Date = row.getAs[Timestamp]("document_version1_date")
      val doc2Date = row.getAs[Timestamp]("document_version2_date")
      val doc1Source = row.getAs[String]("doc1_source")
      val doc2Source = row.getAs[String]("doc2_source")

      var ret = false

      if (doc1Type.startsWith("PROPOSAL")) {
        ret = doc1Date.toLocalDateTime.getYear <= doc2Date.toLocalDateTime.getYear
      }

      if (doc2Type.startsWith("PROPOSAL")) {
        ret = doc2Date.toLocalDateTime.getYear <= doc1Date.toLocalDateTime.getYear
      }

      ret
    }
  }

  override def name: String = "Date filter"
}
