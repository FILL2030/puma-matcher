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
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

class DualTypeEntitiesMatcher(
                               var primaryEntityType: EntityType,
                               var secondaryEntityType: EntityType,
                               var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                               var dbProperties: Properties = ProgramConfig.dbProperties) extends Matcher {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (EntitySimilarityDfType, this.matchData(input.head._2))
  }

  def matchData(data: DataFrame): DataFrame = {
    import data.sparkSession.implicits._

    //get entities count
    val entitiesCountPerDocument = data.groupBy("document_version_id").agg(count("entity_id") as "entity_count").join(data, "document_version_id")

    //get term frequency
    var TF = entitiesCountPerDocument.groupBy("entity_id", "document_version_id", "entity_count", "type").count()
      .withColumn("TF", struct($"document_version_id", $"count", $"entity_count")).select("entity_id", "document_version_id", "type", "TF")
      .cache()

    //spilt over type
    val primaryTF = TF.filter($"type" === primaryEntityType.stringValue)
    val secondaryTF = TF.filter($"type" === secondaryEntityType.stringValue)

    //index entities and sort index to avoid duplicated combination
    var primaryIndex = primaryTF.groupBy("entity_id").agg(sort_array(collect_list("TF")) as "primary_TF").toDF("entity_id", "primary_TF")
    var secondaryIndex = secondaryTF.groupBy("entity_id").agg(sort_array(collect_list("TF")) as "secondary_TF").toDF("entity_id", "secondary_TF")

    //main index
    var entitiesIndex = primaryIndex.join(secondaryIndex, "entity_id")

    //compute IDF
    entitiesIndex = entitiesIndex.withColumn("IDF", log10(lit(entitiesCountPerDocument.count) / (size($"primary_TF") + size($"secondary_TF"))))

    //generate similarities
    val similaritiesDF = entitiesIndex.flatMap(generateSimilaritiesFMF, similaritiesEncoder)

    //return
    similaritiesDF
  }

  val similaritiesEncoder = RowEncoder.apply(StructType(
    Array(
      StructField("document_version1_id", LongType, nullable = false),
      StructField("document_version2_id", LongType, nullable = false),
      StructField("document_version1_tf", LongType, nullable = false),
      StructField("document_version2_tf", LongType, nullable = false),
      StructField("document_version1_entity_count", LongType, nullable = false),
      StructField("document_version2_entity_count", LongType, nullable = false),
      StructField("entity_id", LongType, nullable = false),
      StructField("idf", DoubleType, nullable = false),
      StructField("score_type", StringType, nullable = false)
    )
  ))

  def generateSimilaritiesFMF = new FlatMapFunction[Row, Row]() {
    override def call(row: Row): util.Iterator[Row] = {
      val rows = new util.ArrayList[Row]()

      //retrieve data
      val entityId = row.getLong(0)
      val primaryTf = row.getAs[mutable.WrappedArray[GenericRowWithSchema]](1)
      val secondaryTf = row.getAs[mutable.WrappedArray[GenericRowWithSchema]](2)
      val idf = row.getDouble(3)

      //generate primary to secondary sim $"document_version_id", $"count", $"entity_count"
      for (primary <- primaryTf; secondary <- secondaryTf) {
        val document_version1Id = primary.getLong(0)
        val document_version1Tf = primary.getLong(2)
        val document_version1EntityCount = primary.getLong(1)

        val document_version2Id = secondary.getLong(0)
        val document_version2Tf = secondary.getLong(2)
        val document_version2EntityCount = secondary.getLong(1)

        if(document_version1Id < document_version2Id){
          rows.add(Row.fromTuple((document_version1Id, document_version2Id, document_version1Tf, document_version2Tf, document_version1EntityCount, document_version2EntityCount, entityId, idf, primaryEntityType.stringValue)))
        }else{
          rows.add(Row.fromTuple((document_version2Id, document_version1Id, document_version2Tf, document_version1Tf, document_version2EntityCount, document_version1EntityCount, entityId, idf, primaryEntityType.stringValue)))
        }
      }

      //generate secondary to secondary sim
      for (pair <- secondaryTf.combinations(2)) {
        val document_version1Id = pair(0).getLong(0)
        val document_version1Tf = pair(0).getLong(2)
        val document_version1EntityCount = pair(0).getLong(1)

        val document_version2Id = pair(1).getLong(0)
        val document_version2Tf = pair(1).getLong(2)
        val document_version2EntityCount = pair(1).getLong(1)

        if(document_version1Id < document_version2Id){
          rows.add(Row.fromTuple((document_version1Id, document_version2Id, document_version1Tf, document_version2Tf, document_version1EntityCount, document_version2EntityCount, entityId, idf, secondaryEntityType.stringValue)))
        }else{
          rows.add(Row.fromTuple((document_version2Id, document_version1Id, document_version2Tf, document_version1Tf, document_version2EntityCount, document_version1EntityCount, entityId, idf, secondaryEntityType.stringValue)))
        }
      }

      //return
      rows.iterator
    }
  }

  override def name: String = "Dual entities matcher"

  override def validInputType: List[DataFrameType] = EntitiesDfType :: Nil

  override def maxInputNumber: Int = 1
}
