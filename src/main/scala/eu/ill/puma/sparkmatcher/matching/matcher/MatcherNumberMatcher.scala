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

import eu.ill.puma.sparkmatcher.matching.pipepline._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

class MatcherNumberMatcher extends Matcher {
  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    val matchCandidate = input.head._2

    import matchCandidate.sparkSession.implicits._

    val idUdf = udf[Int, String](x => EntityType.getByValue(x).id)

    //compute matcher number score
    val matchNumberMatcherCandidate = matchCandidate.groupBy($"document_version1_id", $"document_version2_id")
      .agg(
        count($"score").cast(DoubleType) as "score",
        count($"score") as "item_count",
        first($"pair_id") as "pair_id"
      )
      .withColumn("score_type", lit(MatcherNumberType.stringValue))
      .withColumn("id", ($"document_version1_id" * 1000000 + $"document_version2_id") * 100 + idUdf($"score_type"))
      .select($"id", $"document_version1_id", $"document_version2_id", $"score_type", $"score", $"item_count")

    //    matchNumberMatcherCandidate.filter($"document_version1_id" === 46591 || $"document_version2_id" === 46591).orderBy($"document_version1_id", $"document_version2_id").show(200)

    (MatchCandidateWithoutTypeDfType, matchNumberMatcherCandidate)
  }

  override def name: String = "match number matcher"

  override def maxInputNumber: Int = 1

  override def validInputType: List[DataFrameType] = MatchCandidateDfType :: Nil
}
