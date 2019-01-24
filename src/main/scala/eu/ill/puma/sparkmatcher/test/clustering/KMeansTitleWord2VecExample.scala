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
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, count, explode, row_number}
import org.apache.spark.storage.StorageLevel
import ClusteringUtils._

object KMeansTitleWord2VecExample {

  def vectorSize = 100

  def clusterNumber = 10

  def clusterLabelSize = 10

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
    //
    //    val wordsData = dataSourceStorage.titleDataSource.loadData()._2
    //      .select($"entity" as "text", $"document_version_id" as "id")
    //      .map(processInputDataMapFunction, RowEncoder.apply(schema))

    val wordsData = dataSourceStorage.abstractDataSource.loadData()._2
      .select($"text", $"document_version_id" as "id")
      .map(processInputDataMapFunction, RowEncoder.apply(schema))


    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("features")
      .setVectorSize(vectorSize)
      .setMinCount(0)

    val model = word2Vec.fit(wordsData)

    val features = model.transform(wordsData)

    /**
      * Cluster data
      */

    // Trains a k-means model.
    val kMeans = new KMeans()
      .setK(clusterNumber)
      .setSeed(1L)
      .setPredictionCol("cluster")


    val kMeansModel = kMeans.fit(features)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    //    val WSSSE = kMeansModel.computeCost(features)
    //    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    //    println("Cluster Centers: ")
    //    kMeansModel.clusterCenters.foreach(println)
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
