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
import org.apache.spark.sql.functions.{count, udf, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode}

class PictureScorer(
                     var saveSimilarities: Boolean = false,
                     var distanceThreshold: Int = ProgramConfig.pictureThreshold,
                     var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                     var dbProperties: Properties = ProgramConfig.dbProperties) extends Scorer {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (MatchCandidateWithoutTypeDfType, this.score(input.head._2))
  }

  def score(data:  DataFrame): DataFrame = {
    import data.sparkSession.implicits._

    //id generation function
    val idUdf = udf[Int, String](x => EntityType.getByValue(x).id)

    //distance function
    val hamming = udf((h1: String, h2: String) => Hamming.getDistance(h1, h2))

    val matchedPicture = data
      .withColumn("id", ($"document_version1_id" * 1000000 + $"document_version2_id") * 100 + idUdf($"score_type"))
      .withColumn("distance", hamming($"doc1_hash", $"doc2_hash"))
      .filter($"distance" <= distanceThreshold)
      .cache()

    //score each picture pair
    val similarities = matchedPicture.select($"id" as "match_id", $"document_version1_id", $"document_version2_id", $"doc1_entity_id", $"doc2_entity_id", $"score_type", $"distance", $"approx")

    //group result => matchCandidate
    val matchCandidate = matchedPicture
      .groupBy($"id", $"document_version1_id", $"document_version2_id", $"score_type").agg(count("*") as "item_count")
      .withColumn("score", lit(100.0))

    //Save similarities
    if (saveSimilarities) {
      similarities.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "picture_similarities", dbProperties)
    }

    //return
    matchCandidate.select($"id", $"document_version1_id", $"document_version2_id", $"score_type", $"score".cast(DoubleType), $"item_count")
  }

  override def name: String = "picture scorer"

  override def maxInputNumber: Int = 1
}

object Hamming {
  /**
    * Calculate the hamming distance between two longs
    *
    * @param hash1 The first hash to compare
    * @param hash2 The second hash to compare
    * @return
    */
  def getDistance(hash1: Long, hash2: Long): Int = {
    //The XOR of hash1 and hash2 is converted to a binary string
    //then the number of '1's is counted. This is the hamming distance
    (hash1 ^ hash2).toBinaryString.count(_ == '1')
  }

  def getDistance(s1: String, s2: String): Int = {
    s1.zip(s2).count(pair => pair._1 != pair._2)
  }
}
