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

import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer

class PictureMatcher(val pictureMinHeight: Int = ProgramConfig.pictureMinHeight,
                     val pictureMinWidth: Int = ProgramConfig.pictureMinWidth,
                     val vectorSize: Int = ProgramConfig.pictureVectorSize,
                     val approxThreshold: Double = ProgramConfig.pictureApproxThreshold) extends Matcher {
  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (PictureSimilarityDfType, this.matchData(input.head._2))
  }

  def matchData(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._

    //filter small picture
    val data = input.filter($"width" >= pictureMinWidth).filter($"height" >= pictureMinHeight)

    //generate feature
    val feature = data.map(processInputData, processInputDataEncoder) //.cache()

    //build lsh model
    //    val brp = new BucketedRandomProjectionLSH().setBucketLength(3).setNumHashTables(8).setOutputCol("values")

    val mh = new MinHashLSH().setNumHashTables(3).setOutputCol("values")

    val model = mh.setInputCol("feature").fit(feature)
    val result = model.approxSimilarityJoin(feature, feature, approxThreshold).withColumn("score_type", lit(PictureType.stringValue)).select(
      $"datasetA.entity_id" as "doc1_entity_id",
      $"datasetB.entity_id" as "doc2_entity_id",
      $"distCol" as "approx",
      $"score_type",
      $"datasetA.document_version_id" as "document_version1_id",
      $"datasetA.hash" as "doc1_hash",
      $"datasetB.document_version_id" as "document_version2_id",
      $"datasetB.hash" as "doc2_hash")
      .filter($"doc1_entity_id" =!= $"doc2_entity_id")
      .filter($"document_version1_id" < $"document_version2_id")

    result
  }

  def processInputData = new MapFunction[Row, Row]() {
    override def call(row: Row): Row = {

      //get input data
      val documentId = row.getLong(0)
      val entityId = row.getLong(1)
      var hashString = row.getString(5)

      //dHash feature
      if (hashString.size != 256) {
        while (hashString.size < 256) hashString += "0"
      }

      val dataArray = ListBuffer.empty[Double]

      hashString.grouped(vectorSize).foreach(group => {
        dataArray.append(Integer.parseInt(group, 2).toDouble)
      })

      //return row
      Row.fromTuple((entityId, documentId, hashString, Vectors.dense(dataArray.toArray)))
    }
  }

  val processInputDataEncoder = RowEncoder.apply(StructType(
    Array(
      StructField("entity_id", LongType, nullable = false),
      StructField("document_version_id", LongType, nullable = false),
      StructField("hash", StringType, nullable = false),
      StructField("feature", VectorType, nullable = false)
    )))

  override def name: String = "picture matcher"

  override def validInputType: List[DataFrameType] = PictureHashDfType :: Nil

  override def maxInputNumber: Int = 1
}
