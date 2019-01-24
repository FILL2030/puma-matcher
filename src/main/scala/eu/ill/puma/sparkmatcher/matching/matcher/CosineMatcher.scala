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

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline.{CosineSimilarityDfType, DataFrameType, EntitiesDfType, EntityType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.nlp.PorterStemmer
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CosineMatcher(var fileDataSource: DataSource,
                    var partitionNumber: Int = ProgramConfig.partitionNumber,
                    var cosineMinScore: Double = ProgramConfig.cosineMinScore,
                    var stopWords: List[String] = ProgramConfig.stopwords) extends Matcher {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (CosineSimilarityDfType, this.matchData(input.head._2))
  }

  def matchData(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._

    //proccess input text
    val featureEncoder = RowEncoder.apply(StructType(
      Array(
        StructField("text", StringType, nullable = false),
        StructField("document_version_id", LongType, nullable = false),
        StructField("text_words", ArrayType(StringType), nullable = false),
        StructField("feature", ArrayType(StringType), nullable = false),
        StructField("position", ArrayType(IntegerType), nullable = false)
      )
    ))

    val modelTextDataFrame: Dataset[(Long, Array[String])] = fileDataSource.loadData._2.select($"document_version_id".as[Long], $"feature".as[Array[String]])

    val feature = input.map(processInputDataMapFunction, featureEncoder)

    //generate idx
    val data = feature
      .withColumn("idx", row_number().over(Window.orderBy($"document_version_id".asc)) - 1)
      .select("idx", "feature", "document_version_id", "position").repartition(partitionNumber)
      .orderBy($"idx".asc).cache

    val dataRdd = data.rdd.map(row => row.getSeq[String](1))

    //TF
    val hashingTF = new HashingTF()

    //generate model
    val modelData = modelTextDataFrame.rdd.map(row => row._2.toSeq)
    val tfModel = hashingTF.transform(modelData)

    val titleTf = hashingTF.transform(dataRdd)

    //IDF
    val idf = new IDF().fit(tfModel)

    //TF IDF
    val tfidf = idf.transform(titleTf)

    //Sim matrix
    val irm = new IndexedRowMatrix(tfidf.zipWithIndex.map {
      case (v, i) => IndexedRow(i, v)
    })

    //result data
    val simDf = irm
      .toCoordinateMatrix
      .transpose
      .toRowMatrix()
      .columnSimilarities()
      .entries
      .filter(_.value > cosineMinScore)
      .toDF()
      .cache()

    //merge data
    var result = data.join(simDf, $"i" === $"idx", "left_outer")
      .drop("idx")
      .withColumnRenamed("value", "cosine_sim")
      .withColumnRenamed("document_version_id", "document_version1_id")
      .withColumnRenamed("feature", "document_version1_feature")
      .withColumnRenamed("position", "document_version1_position")

    result = data.join(result, $"j" === $"idx")
      .drop("i", "j", "idx")
      .withColumnRenamed("document_version_id", "document_version2_id")
      .withColumnRenamed("feature", "document_version2_feature")
      .withColumnRenamed("position", "document_version2_position")

    //detect common words
    val encoder = RowEncoder.apply(StructType(
      Array(
        StructField("document_version1_id", LongType, nullable = false),
        StructField("document_version2_id", LongType, nullable = false),
        StructField("document_version1_feature", ArrayType(StringType), nullable = false),
        StructField("document_version2_feature", ArrayType(StringType), nullable = false),
        StructField("document_version1_position", ArrayType(IntegerType), nullable = false),
        StructField("document_version2_position", ArrayType(IntegerType), nullable = false),
        StructField("cosine_sim", DoubleType),
        StructField("common_feature", ArrayType(StringType), nullable = false)
      )
    ))

    result.map(processOutputDataMapFunction, encoder)
  }

  def processInputDataMapFunction = new MapFunction[Row, Row]() {
    override def call(row: Row): Row = {
      //input data
      val text = row.getAs[String]("entity")
      val documentId = row.getAs[Long]("document_version_id")

      //word spilt
      val words = text.split("\\W+")

      //stop word removal
      val (filteredWords, positions) = words.zipWithIndex.filter(word => !stopWords.contains(word._1)).unzip

      //stemming
      val stemFeature = PorterStemmer.stem(filteredWords).filter(_.length != 1)

      //output
      Row.fromTuple((text, documentId, words, stemFeature, positions))
    }
  }

  def processOutputDataMapFunction = new MapFunction[Row, Row]() {
    override def call(row: Row): Row = {
      val document_version1Id = row.getAs[Long]("document_version1_id")
      val document_version2Id = row.getAs[Long]("document_version2_id")

      val document_version1Feature = row.getAs[mutable.WrappedArray[String]]("document_version1_feature")
      val document_version2Feature = row.getAs[mutable.WrappedArray[String]]("document_version2_feature")

      val document_version1Position = row.getAs[mutable.WrappedArray[Integer]]("document_version1_position")
      val document_version2Position = row.getAs[mutable.WrappedArray[Integer]]("document_version2_position")

      var cosineSim = row.getAs[Double]("cosine_sim")
      if (cosineSim > 1) cosineSim = 1

      val commonsFeature = document_version1Feature.intersect(document_version2Feature)

      val document_version1CommonPosition = new ListBuffer[Integer]
      val document_version2CommonPosition = new ListBuffer[Integer]

      commonsFeature.foreach(word => {
        val doc1Pos = document_version1Position(document_version1Feature.indexOf(word))
        val doc2Pos = document_version2Position(document_version2Feature.indexOf(word))
        document_version1CommonPosition += doc1Pos
        document_version2CommonPosition += doc2Pos
      })

      Row.fromTuple((document_version1Id, document_version2Id, document_version1Feature, document_version2Feature, document_version1CommonPosition, document_version2CommonPosition, cosineSim, commonsFeature))
    }
  }

  override def name: String = "Cosine matcher"

  override def validInputType: List[DataFrameType] = EntitiesDfType :: Nil

  override def maxInputNumber: Int = 1
}
