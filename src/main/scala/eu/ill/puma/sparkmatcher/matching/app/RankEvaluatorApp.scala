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
package eu.ill.puma.sparkmatcher.matching.app

import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object RankEvaluatorApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)


    this.run
  }

  def run: Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Matcher")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()


    import sparkSession.implicits._

    val sql = "(select proposal_id, publication_id, match_candidate.id, score_type, score, (select max(x) from unnest(scores) x) max_score, (select min(x) from unnest(scores) x) min_score, rank, size, document_type " +
      " from matching_static.test_match, match_candidate " +
      " ,(select unnest(rank) as rank,array_length(rank,1) as size, unnest(match_ids) as match_id,scores, document_type from match_candidate_stats ) stats" +
      " where (proposal_id = document_version1_id and publication_id = document_version2_id or proposal_id = document_version2_id and publication_id = document_version1_id)" +
      " and stats.match_id = match_candidate.id" +
      " order by match_candidate.id ) query"

    var matchCandidatStats = sparkSession.read.jdbc(ProgramConfig.matchingDatabaseUrl, sql, ProgramConfig.dbProperties)

    val result = matchCandidatStats.groupBy("score_type").agg(lit("ALL") as "document_type", count("*") / 2 as "count")

    result.write.mode(SaveMode.Overwrite).jdbc(ProgramConfig.matchingDatabaseUrl, ProgramConfig.startSchema + ".score_rank_evaluator", ProgramConfig.dbProperties)

    result.filter($"document_type" === "ALL").show(2000)
  }
}
