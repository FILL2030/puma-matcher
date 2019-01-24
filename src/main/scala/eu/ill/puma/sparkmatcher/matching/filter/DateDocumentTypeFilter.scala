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

class DateDocumentTypeFilter(dateDataSource: DataSource, startYear: Int, endYear: Int) extends Filter {

  override def run(input: DataFrame): DataFrame = {

    import input.sparkSession.implicits._

    val dateDataFrame = dateDataSource.loadData._2.na.fill(Map("date" -> "1900-01-01 00:00:00"))

    input
      .withColumn("publication_id", when($"document1_type" === "publication", $"document_version1_id").otherwise($"document_version2_id"))
      .join(dateDataFrame, $"publication_id" === "document_version_id")
      .filter(year($"date" >= startYear && year($"date") <= endYear))
      .drop("publication_id", "date", "document_version_id", "document_type")
  }

  override def name: String = "Date document type filter"
}
