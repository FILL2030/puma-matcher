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
package eu.ill.puma.sparkmatcher.test.clustering

import edu.stanford.nlp.process.Morphology
import edu.stanford.nlp.simple.Document
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.functions.{broadcast, sum, udf, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ClusteringUtils {
  var schema = StructType(
    Array(
      StructField("id", LongType, nullable = false),
      StructField("words", ArrayType(StringType), nullable = false)
    )
  )

  def processInputDataMapFunction = new MapFunction[Row, Row]() {
    override def call(row: Row): Row = {
      //input data
      val text = row.getAs[String]("text").replaceAll("[^A-Za-z0-9 ]", " ");
      val id = row.getAs[Long]("id")

      //nlp variable
      val morphology = new Morphology()
      val document = new Document(text)
      val sentences = document.sentences()
      val sentenceStem = ListBuffer.empty[String]
      val sentenceLem = ListBuffer.empty[String]

      import scala.collection.JavaConversions._

      //nlp processing
      document.sentences().foreach(sentence => {
        val words = sentence.words()
        val tags = sentence.posTags()

        for ((word, tag) <- (words zip tags)) {
          val stem = morphology.stem(word)
          val lem = morphology.lemma(word, tag)

          if (!ProgramConfig.stopwords.contains(word) &&
            !ProgramConfig.stopwords.contains(stem) &&
            !ProgramConfig.stopwords.contains(lem) &&
            word.length > 2) {
            sentenceStem += stem
            sentenceLem += lem
          }
        }
      })

      //return
      Row.fromTuple((id, sentenceLem))
    }
  }

  /**
    * return the commons items of two string arrays
    * @return
    */
  def commons_words = udf { (a: mutable.WrappedArray[String], b: mutable.WrappedArray[String]) => a.intersect(b) }




  def array_distinct = udf { (a: mutable.WrappedArray[String]) => a.distinct}

  /**
    * apply the possible weight to the match candidate
    *
    * @param matchCandidate  the match candidate where the weight are applied
    * @param weightDataFrame the weight dataframe
    * @return
    */
  def computeWeight(matchCandidate: DataFrame, weightDataFrame: DataFrame): DataFrame = {

    import matchCandidate.sparkSession.implicits._

    val preparedMatchCandidate = matchCandidate.select(
      $"id",
      when($"document_version1_id" < $"document_version2_id", $"document_version1_id").otherwise($"document_version2_id") as "document_version1_id",
      when($"document_version2_id" > $"document_version1_id", $"document_version2_id").otherwise($"document_version1_id") as "document_version2_id",
      $"score_type",
      $"score")

    val weight = broadcast(weightDataFrame)


    //apply weight
    val matchCandidateWithWeight = preparedMatchCandidate.join(weight, Seq("score_type"))
      .withColumn("weight_score", $"score" * $"weight")
      .drop("score", "weight", "score_type")
      .repartition($"document_version1_id", $"document_version2_id")

    //compute total score
    val totalMatchCandidate = matchCandidateWithWeight.groupBy($"document_version1_id", $"document_version2_id").agg(
      sum($"weight_score") as "score"
    )

    totalMatchCandidate
  }
}
