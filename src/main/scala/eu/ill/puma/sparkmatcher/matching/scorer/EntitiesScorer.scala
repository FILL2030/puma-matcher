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

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntityType, MatchCandidateWithoutTypeDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

class EntitiesScorer(var saveSimilarities: Boolean = true,
                     var scoreFactor: Int = ProgramConfig.scoreFactor,
                     var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                     var dbProperties: Properties = ProgramConfig.dbProperties) extends Scorer {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (MatchCandidateWithoutTypeDfType, this.score(input.head._2))
  }

  def score(data:  DataFrame): DataFrame = {
    import data.sparkSession.implicits._

    val idUdf = udf[Int, String](x => EntityType.getByValue(x).id)

    //score each entities
    val entitiesSimilaritiesWithTFIDFScore = data.withColumn("score", ($"document_version1_tf" * $"idf" + $"document_version2_tf" * $"idf"))

    //group them
    val similarityWithTeamScore = entitiesSimilaritiesWithTFIDFScore
      .groupBy("document_version1_id", "document_version2_id", "document_version1_entity_count", "document_version2_entity_count", "score_type")
      .agg(
        sum("score") as "total_score",
        count("score") as "common_entities_count",
        collect_list("entity_id") as "entities_ids",
        collect_list("score") as "entities_score"
      )

    val similarityWithFinalScore = similarityWithTeamScore
      .withColumn("team_factor", $"common_entities_count" * ((lit(0.5) / $"document_version1_entity_count") + (lit(0.5) / $"document_version2_entity_count"))).drop("score")
      .withColumn("score", $"total_score" * (lit(1) + (lit(scoreFactor) * $"team_factor")))
      .withColumn("id", ($"document_version1_id" * 1000000 + $"document_version2_id") * 100 + idUdf($"score_type"))
      .drop("document_version1_entity_count", "document_version2_entity_count", "total_score")

    val finalScoreDF = similarityWithFinalScore.groupBy("document_version1_id", "document_version2_id", "id", "score_type").agg(
      sum("common_entities_count") as "item_count",
      sum("score") as "score")

    if (saveSimilarities) {
      similarityWithFinalScore
        .select($"document_version1_id", $"document_version2_id", $"id" as "match_id", $"score_type" as "type", $"common_entities_count" as "entities_count", $"entities_ids", $"entities_score", $"team_factor" as "score_factor", $"score")
        .write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "entities_similarities", dbProperties)
    }

    //return
    finalScoreDF.select($"id", $"document_version1_id", $"document_version2_id", $"score_type", $"score", $"item_count")
  }

  override def name: String = "entities scorer"

  override def maxInputNumber: Int = 1
}
