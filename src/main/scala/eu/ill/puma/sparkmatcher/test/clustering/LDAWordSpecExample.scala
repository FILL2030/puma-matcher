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

import java.util

import eu.ill.puma.sparkmatcher.matching.datasource.DataSourceStorage
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.log4j.Level
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{broadcast, collect_list, explode, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object LDAWordSpecExample {

  def clusterNumber = 15

  def iteration = 5

  def topicSize = 12

  def wordMinDocumentFrequency = 1

  def wordMinTermFrequency = 1

  def run = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.WARN)
    org.apache.log4j.Logger.getLogger("edu").setLevel(Level.ERROR)


    /**
      * spark setup
      */

    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Matcher")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()


    import sparkSession.implicits._

    //create data source storage
    val dataSourceStorage = new DataSourceStorage(sparkSession)

    /**
      * process input data
      */

    val wordsData = dataSourceStorage.documentWordSpecDataSource.loadData()._2
      .select($"entities" as "words", $"document_version_id" as "id")
      .groupBy($"id").agg(collect_list($"words") as "words")

    // fit a CountVectorizerModel from the corpus
    val CVModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(1000000)
      .setMinDF(wordMinDocumentFrequency)
      .setMinTF(wordMinTermFrequency)
      .fit(wordsData)

    //apply cv model to data
    val features = CVModel.transform(wordsData).cache()

    //extract words index
    val wordDF = broadcast(sparkSession.createDataFrame(CVModel.vocabulary.zipWithIndex).toDF("word", "word_index"))

    /**
      * Clustering data
      */

    // Trains a LDA model.
    val lda = new LDA()
      .setK(clusterNumber)
      .setMaxIter(iteration)


    //apply LDA model
    val LDAModel = lda.fit(features)

    // Describe topics.
    val topic = LDAModel
      .describeTopics(topicSize)
      .repartition(ProgramConfig.partitionNumber, $"topic")
      .cache()
      .select($"topic", explode($"termIndices") as "term_index", $"termWeights")
      .join(wordDF, $"term_index" === $"word_index")
      .groupBy($"topic", $"termWeights")
      .agg(collect_list($"word") as "word")
      .orderBy($"topic")
      .select($"topic", $"word" as "topicWords", $"termWeights")
      .cache()

    // apply each topic foreach document
    val transformed = LDAModel.transform(features)
      .withColumn("topicIndex", lit(0 until clusterNumber toArray))
      .select($"id", $"words", $"topicDistribution", $"topicIndex")
      .flatMap(LDA_FMF, LDAEncoder)
      .cache()

    //get result foreach doc
    val result = topic.join(transformed, $"topicIndex" === $"topic")
      .select($"id", $"words", $"topicIndex", $"topicWords", $"topicProbabilty", $"termWeights")
      .orderBy($"topicIndex")
      .cache()


    topic.join(result.groupBy($"topicWords").count(), Seq("topicWords"))
      .drop("termWeights")
      .orderBy($"topic")
      .show(clusterNumber, false)


    println("topic number      : " + clusterNumber)
    println("topic size        : " + topicSize)
    println("iteration         : " + iteration)
    println("document          : " + wordsData.count())
    println("probability > 0.8 : " + result.filter($"topicProbabilty" > 0.8).count())
    println("probability > 0.6 : " + result.filter($"topicProbabilty" > 0.6).count())
    println("probability > 0.4 : " + result.filter($"topicProbabilty" > 0.4).count())
    println("probability > 0.2 : " + result.filter($"topicProbabilty" > 0.2).count())
    println("probability > 0.0 : " + result.count())

  }

  def LDAEncoder = RowEncoder.apply(StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("words", ArrayType(StringType), nullable = false),
    StructField("topicIndex", LongType, nullable = false),
    StructField("topicProbabilty", DoubleType, nullable = false)
  )))

  def LDA_FMF = new FlatMapFunction[Row, Row]() {
    override def call(row: Row): util.Iterator[Row] = {

      //init
      val rows = new util.ArrayList[Row]()
      val id = row.getAs[Long]("id")
      val words = row.getAs[mutable.WrappedArray[String]]("words")
      val topicDistribution = row.getAs[DenseVector]("topicDistribution")

      var totalProbabilty = 0.0
      var first = true
      //explode
      topicDistribution.values.zipWithIndex.sortBy(-_._1) foreach (x => {
        val topicProbabilty = x._1
        val topicIndex = x._2.toLong
        totalProbabilty = totalProbabilty + topicProbabilty

        if (totalProbabilty <= 0.8 || first) rows add Row.fromTuple((id, words, topicIndex, topicProbabilty))

        first = false
      })

      //return
      rows.iterator
    }
  }
}
