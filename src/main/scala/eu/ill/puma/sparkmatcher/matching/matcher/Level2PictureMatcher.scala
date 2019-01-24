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
package eu.ill.puma.sparkmatcher.matching.matcher

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

class Level2PictureMatcher(val matchCandidateDataSource: DataSource,
                           val pictureMinHeight: Int = ProgramConfig.pictureMinHeight,
                           val pictureMinWidth: Int = ProgramConfig.pictureMinWidth) extends Matcher {
  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (PictureSimilarityDfType, this.matchData(input.head._2))
  }

  def matchData(input:  DataFrame): DataFrame = {
    import input.sparkSession.implicits._


    val hash = input.filter($"width" >= pictureMinWidth).filter($"height" >= pictureMinHeight)
    val matchCandidate = matchCandidateDataSource.loadData._2


    val data = matchCandidate.select($"document_version1_id" as "old_document_version1_id", $"document_version2_id" as "old_document_version2_id")
      .withColumn("document_version1_id", when($"old_document_version1_id" < $"old_document_version2_id", $"old_document_version1_id").otherwise($"old_document_version2_id"))
      .withColumn("document_version2_id", ($"old_document_version1_id" * $"old_document_version2_id" / $"document_version1_id").cast(LongType))
      .drop("old_document_version1_id", "old_document_version2_id")
      .distinct()
      .join(hash, $"document_version1_id" === $"document_version_id").drop("id", "document_version_id", "file_path", "width", "height")
      .withColumnRenamed("entity_id", "doc1_entity_id")
      .withColumnRenamed("hash", "doc1_hash")
      .join(hash, $"document_version2_id" === $"document_version_id").drop("id", "document_version_id", "file_path", "width", "height")
      .withColumnRenamed("entity_id", "doc2_entity_id")
      .withColumnRenamed("hash", "doc2_hash")
      .withColumn("score_type", lit(PictureType.stringValue))
      .withColumn("approx", lit(-1.0))
      .select($"doc1_entity_id", $"doc2_entity_id", $"approx", $"score_type", $"document_version1_id", $"doc1_hash", $"document_version2_id", $"doc2_hash")

    data
  }


  override def name: String = "picture matcher"

  override def validInputType: List[DataFrameType] = PictureHashDfType :: Nil

  override def maxInputNumber: Int = 1
}
