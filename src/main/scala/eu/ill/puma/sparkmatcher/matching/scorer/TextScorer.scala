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

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntityType, MatchCandidateDfType, MatchCandidateWithoutTypeDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, udf}

import scala.collection.mutable

class TextScorer(var sentenceLength: Int = ProgramConfig.sentenceLength) extends Scorer {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (MatchCandidateWithoutTypeDfType, this.score(input.head._2, entityType))
  }

  def score(data: DataFrame, entityType: EntityType): DataFrame = {
    import data.sparkSession.implicits._

    val scoreTextUdf = udf[Double, mutable.WrappedArray[Long]](_ match {
      case a: Any => {
        var classicScore: Double = 0
        for (size <- a) {
          classicScore += size - (sentenceLength - 1)
        }
        classicScore
      }
      case _ => 0.toDouble
    })

    val idUdf = udf[Int, String](x => EntityType.getByValue(x).id)

    val textScore = data
      .withColumn("score_type", lit(entityType.stringValue))
      .withColumn("score", scoreTextUdf($"sentences_size"))
      .withColumn("id", ($"document_version1_id" * 1000000 + $"document_version2_id") * 100 + idUdf($"score_type"))

    textScore.select($"id", $"document_version1_id", $"document_version2_id", $"score_type", $"score", $"word_match_count" as "item_count")
  }

  override def name: String = "Text scorer"

  override def maxInputNumber: Int = 1
}
