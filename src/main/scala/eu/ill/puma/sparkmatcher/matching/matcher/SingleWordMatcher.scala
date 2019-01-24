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

import java.util
import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntitiesDfType, EntitySimilarityDfType, EntityType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import eu.ill.puma.sparkmatcher.utils.nlp.PorterStemmer
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

class SingleWordMatcher(
                         var minWordLength: Int = ProgramConfig.titleMinimumWordLength,
                         var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                         var dbProperties: Properties = ProgramConfig.dbProperties,
                         var stopWords: List[String] = ProgramConfig.stopwords) extends Matcher {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (EntitySimilarityDfType, this.matchData(input.head._2, entityType))
  }

  def matchData(data: DataFrame, entityType: EntityType): DataFrame = {
    import data.sparkSession.implicits._

    val documentNumber = data.count()

    //proccess input text
    val feature = data.flatMap(processInputDataMapFunction, featureEncoder).cache()

    //index entities and sort index to avoid duplicated combination
    var entitiesIndex = feature.groupBy("word").agg(sort_array(collect_list(struct("document_version_id", "position", "tf"))) as "data")
      .withColumn("idf", sqrt(lit(documentNumber) / size($"data")))
      .cache()

    //generate similarities
    val rightData = entitiesIndex.select($"idf", $"word", explode($"data") as "right_data")
    val leftData = entitiesIndex.select($"word", explode($"data") as "left_data")

    val similarities = rightData.join(leftData, Seq("word"))
      .filter($"right_data.document_version_id" < $"left_data.document_version_id")
      .select(
        $"right_data.document_version_id" as "document_version1_id",
        $"left_data.document_version_id" as "document_version2_id",
        $"right_data.tf" as "document_version1_tf",
        $"left_data.tf" as "document_version2_tf",
        $"right_data.position" as "document_version1_position",
        $"left_data.position" as "document_version2_position",
        $"idf",
        lit(entityType.stringValue) as "score_type",
        $"word")
      .cache()

    //compute full idf
    val documentWords = feature.select($"word", $"document_version_id")
    val wordsIdf = entitiesIndex.select($"word", $"idf")

    val documentIdf = documentWords.join(wordsIdf, Seq("word")).groupBy("document_version_id").agg(sum("idf") as "total_idf")

    //result
    val result = similarities
      .join(documentIdf, $"document_version1_id" === $"document_version_id").withColumnRenamed("total_idf", "document_version1_total_idf").drop("document_version_id")
      .join(documentIdf, $"document_version2_id" === $"document_version_id").withColumnRenamed("total_idf", "document_version2_total_idf").drop("document_version_id")

    //return
    result
  }

  def processInputDataMapFunction = new FlatMapFunction[Row, Row]() {
    override def call(row: Row): util.Iterator[Row] = {
      val rows = new util.ArrayList[Row]()

      //input data
      val text = row.getAs[String]("entity")
      val documentId = row.getAs[Long]("document_version_id")

      //word spilt
      val words = text.split("\\W+")

      //stop word removal
      val (filteredWords, positions) = words.zipWithIndex
        .filter(word => !stopWords.contains(word._1))
        .filter(word => word._1.length >= minWordLength)
        .unzip

      //stemming
      val stemFeature = PorterStemmer.stem(filteredWords)

      //create rows
      for (i <- stemFeature.indices) {
        rows.add(Row.fromTuple((stemFeature(i), documentId, stemFeature.size, positions(i), stemFeature.count(_ == stemFeature(i)))))
      }

      //row
      rows.iterator()
    }
  }

  val featureEncoder = RowEncoder.apply(StructType(
    Array(
      StructField("word", StringType, nullable = false),
      StructField("document_version_id", LongType, nullable = false),
      StructField("document_entity_count", IntegerType, nullable = false),
      StructField("position", IntegerType, nullable = false),
      StructField("tf", IntegerType, nullable = false)

    )
  ))

  override def name: String = "Word matcher"

  override def validInputType: List[DataFrameType] = EntitiesDfType :: Nil

  override def maxInputNumber: Int = 1
}
