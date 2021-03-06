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

import eu.ill.puma.sparkmatcher.matching.datasource.DataSourceStorage
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.log4j.Level
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import ClusteringUtils._

object KMeansTitleTfIdfExample {

  def clusterNumber = 20

  def clusterLabelSize = 10

  def wordMinDocumentFrequency = 1

  def wordMinTermFrequency = 1

  def run = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)


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

    //    val wordsData = dataSourceStorage.titleDataSource.loadData()._2
    //      .select($"entity" as "text", $"document_version_id" as "id")
    //      .map(processInputDataMapFunction, RowEncoder.apply(schema))

    val wordsData = dataSourceStorage.abstractDataSource.loadData()._2
      .select($"text", $"document_version_id" as "id")
      .map(processInputDataMapFunction, RowEncoder.apply(schema))


    // fit a CountVectorizerModel from the corpus
    val CVModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setVocabSize(1000000)
      .setMinDF(wordMinDocumentFrequency)
      .setMinTF(wordMinTermFrequency)
      .fit(wordsData)

    //apply cv model to data
    val featurizedData = CVModel.transform(wordsData)

    //extract words index
    val wordDF = broadcast(sparkSession.createDataFrame(CVModel.vocabulary.zipWithIndex).toDF("word", "word_index"))

    //IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val features = idfModel.transform(featurizedData).drop("rawFeatures").cache()


    /**
      * Cluster data
      */

    // Trains a k-means model.
    val kMeans = new KMeans()
      .setK(clusterNumber)
      .setSeed(1L)
      .setPredictionCol("cluster")


    val kMeansModel = kMeans.fit(features)


    val clusterAssignementWithWords = kMeansModel.transform(features)
      .drop("features")
      .cache()

    //cluster document count
    val clusterDocumentCount = clusterAssignementWithWords
      .groupBy($"cluster").count()
      .select($"cluster", $"count" as "cluster_document_count")
      .cache()

    //add top words to create cluster label
    val clusterWordRank = clusterAssignementWithWords
      .select($"id", explode($"words") as "word", $"cluster")
      .groupBy($"cluster", $"word")
      .agg(collect_list($"id") as "ids", count("*") as "count")
      .withColumn("rank", row_number().over(Window.partitionBy($"cluster").orderBy($"count".desc)))
      .filter($"rank" <= clusterLabelSize)
      .orderBy($"cluster", $"rank")
      .select($"cluster", $"ids", $"count", $"word", $"rank")
      .cache()

    //extract distinct topics
    val topic = clusterWordRank
      .groupBy($"cluster")
      .agg(collect_list($"word") as "top_words", collect_list($"rank") as "top_word_rank")
      .select($"cluster", $"top_words", $"top_word_rank")
      .cache()

    //apply result to document
    val documentWithCluster = topic.join(clusterAssignementWithWords, Seq("cluster"))
      .select($"cluster", $"id", $"top_words", commons_words($"top_words", $"words") as "common_words", $"words", $"top_word_rank")
      .cache()

    //view
    topic
      .join(clusterDocumentCount, Seq("cluster"))
      .show(clusterNumber, false)

    documentWithCluster.show(100, false)


  }
}
