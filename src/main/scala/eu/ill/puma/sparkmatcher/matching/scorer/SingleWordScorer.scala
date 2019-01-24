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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

class SingleWordScorer(var countDuplicatedWords: Boolean = true,
                       var saveSimilarities: Boolean = true,
                       var minMatchedWord: Int = ProgramConfig.titleMinimumMatchedWords,
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
    var score = data

    if (countDuplicatedWords) {
      score = data.withColumn("score", lit(100) * ($"idf" * $"document_version1_tf" + $"idf" * $"document_version2_tf") / ($"document_version1_total_idf" + $"document_version2_total_idf"))
    } else {
      score = data.withColumn("score", lit(100) * ($"idf" / $"document_version1_tf" + $"idf" / $"document_version2_tf") / ($"document_version1_total_idf" + $"document_version2_total_idf"))
    }

    //group them
    val groupedScore = score.groupBy("document_version1_id", "document_version2_id", "score_type")
      .agg(sum("score") as "total_score", count("score") as "item_count", collect_list("document_version1_position") as "document_version1_position", collect_list("document_version2_position") as "document_version2_position", collect_list("word") as "word")
      .withColumn("id", ($"document_version1_id" * 1000000 + $"document_version2_id") * 100 + idUdf($"score_type"))
      .filter(size($"document_version1_position") >= minMatchedWord)
      .filter(size($"document_version2_position") >= minMatchedWord)

    //save similarities
    if (saveSimilarities) {
      groupedScore
        .select($"document_version1_id", $"document_version2_id", $"id" as "match_id", $"score_type" as "type", $"document_version1_position", $"document_version2_position", $"word", $"total_score" as "score")
        .write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "word_similarities", dbProperties)
    }

    //return
    groupedScore.select($"id", $"document_version1_id", $"document_version2_id", $"score_type", $"total_score" as "score", $"item_count")
  }

  override def name: String = "entities scorer"

  override def maxInputNumber: Int = 1
}
