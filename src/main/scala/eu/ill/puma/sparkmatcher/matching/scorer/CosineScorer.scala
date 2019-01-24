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
package eu.ill.puma.sparkmatcher.matching.scorer

import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntityType, MatchCandidateDfType, MatchCandidateWithoutTypeDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class CosineScorer(var saveSimilarities: Boolean = true,
                   var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                   var dbProperties: Properties = ProgramConfig.dbProperties) extends Scorer {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (MatchCandidateWithoutTypeDfType, this.score(input.head._2, entityType))
  }

  def score(data: DataFrame, entityType: EntityType): DataFrame = {
    import data.sparkSession.implicits._

    val similarities = data.select($"document_version1_id", $"document_version2_id", lit(entityType.stringValue) as "type", $"document_version1_feature", $"document_version2_feature", $"document_version1_position", $"document_version2_position", $"cosine_sim" * 100 as "score", $"common_feature")
      .withColumn("match_id", ($"document_version1_id" * 1000000 + $"document_version2_id") * 100 + entityType.id)

    val cosineMatchCandidate = similarities.withColumnRenamed("type", "score_type").withColumnRenamed("cosine_sim", "score").withColumnRenamed("match_id", "id").withColumn("item_count", lit(1L))
      .select($"id", $"document_version1_id", $"document_version2_id", $"score_type", $"score", $"item_count")

    if (saveSimilarities) {
      similarities.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "cosine_similarities", dbProperties)
    }

    cosineMatchCandidate
  }

  override def name: String = "Cosine scorer"

  override def maxInputNumber: Int = 1
}
