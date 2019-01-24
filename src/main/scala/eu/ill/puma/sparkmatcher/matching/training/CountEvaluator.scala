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
package eu.ill.puma.sparkmatcher.matching.training
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, count, lit, when}

class CountEvaluator extends Evaluator {
  override def run(input: DataFrame): Double = {
    import input.sparkSession.implicits._

    val sql = "(select proposal_id, publication_id, match_candidate.id, score_type, score, (select max(x) from unnest(scores) x) max_score, (select min(x) from unnest(scores) x) min_score, rank, size, document_type " +
      " from test_match, match_candidate " +
      " ,(select unnest(rank) as rank,array_length(rank,1) as size, unnest(match_ids) as match_id,scores, document_type from match_candidate_stats ) stats" +
      " where (proposal_id = document_version1_id and publication_id = document_version2_id or proposal_id = document_version2_id and publication_id = document_version1_id)" +
      " and stats.match_id = match_candidate.id" +
      " order by match_candidate.id ) query"

    var matchCandidatStats = input.sparkSession.read.jdbc(ProgramConfig.matchingDatabaseUrl, sql, ProgramConfig.dbProperties).cache()
      .withColumn("rank_percent", when($"size" === 1, 1).otherwise(($"size" - $"rank") / ($"size" - 1)))
      .withColumn("score_percent", when($"score" === $"max_score", 1).otherwise(($"score" - $"min_score") / ($"max_score" - $"min_score")))

    val result = matchCandidatStats.groupBy("score_type", "document_type").agg(avg("rank_percent") as "rank_percent", avg("score_percent") as "score_percent", count("rank_percent") as "count")
      .union(matchCandidatStats.groupBy("score_type").agg(lit("ALL") as "document_type", avg("rank_percent") as "rank_percent", avg("score_percent") as "score_percent", count("rank_percent") as "count"))
    //      .withColumn("score_percent_all_match", $"score_percent" * $"count"  / lit("testMatchCount"))
    //      .withColumn("rank_percent_all_match", $"rank_percent" * $"count"  / lit("testMatchCount"))

//    result.write.mode(SaveMode.Overwrite).jdbc(Config.matchingDatabaseUrl, "matching.score_rank_evaluator", Config.dbProperties)

    result.filter($"document_type" === "ALL").collect()(0).getLong(4).toDouble
  }

  override def lowerIsBetter: Boolean = false
}
