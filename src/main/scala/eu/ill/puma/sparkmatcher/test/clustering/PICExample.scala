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
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, count, explode, row_number}
import org.apache.spark.storage.StorageLevel
import ClusteringUtils._

object PICExample {

  def clusterNumber = 20

  def clusterLabelSize = 10

  def maxIteration = 10

  def run {
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

    //compute total matchcandidate
    val matchCandidate = dataSourceStorage.matchCandidateDataSource.loadData()._2
    val weight = dataSourceStorage.weightDataSource.loadData()._2
    val totalMatchCandidate = computeWeight(matchCandidate, weight)

    //setup input rdd
    val inputRdd = totalMatchCandidate
      .select($"document_version1_id", $"document_version2_id", $"score")
      .rdd
      .map(row => (row.getLong(0), row.getLong(1), row.getDouble(2)))

    //    val wordsDF = dataSourceStorage.titleDataSource.loadData()._2
    //      .select($"entity" as "text", $"document_version_id" as "id")
    //      .map(processInputDataMapFunction, RowEncoder.apply(schema))

    val wordsDF = dataSourceStorage.abstractDataSource.loadData()._2
      .select($"text", $"document_version_id" as "id")
      .map(processInputDataMapFunction, RowEncoder.apply(schema))

    //create model
    val model = new PowerIterationClustering()
      .setK(clusterNumber)
      .setMaxIterations(maxIteration)
      .setInitializationMode("degree")
      .run(inputRdd)

    //get cluster
    val clusterAssignement = model.assignments.toDF()
      .cache()

    //vluster with words
    val clusterAssignementWithWords = clusterAssignement.join(wordsDF, Seq("id"))
      .cache()

    //cluster document count
    val clusterDocumentCount = clusterAssignement
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
