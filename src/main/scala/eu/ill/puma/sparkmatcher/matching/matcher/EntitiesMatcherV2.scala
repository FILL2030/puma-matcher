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

import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntitiesIdDfType, EntitySimilarityDfType, EntityType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class EntitiesMatcherV2(var safeMode: Boolean = true,
                        var maximumEntitiesOccurency: Int = ProgramConfig.maximumEntitiesOccurency,
                        var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                        var dbProperties: Properties = ProgramConfig.dbProperties) extends Matcher {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (EntitySimilarityDfType, this.matchData(input.head._2, entityType))
  }

  def matchData(data: DataFrame, entityType: EntityType): DataFrame = {
    import data.sparkSession.implicits._

    //get documentNumber
    val documentNumber = data.select($"document_version_id").distinct().count()

    //remove entity wifi to high frequency to avoid major performance issue
    var safeData = data

    if (safeMode) {
      safeData = data.groupBy("entity_id").agg(
        collect_list($"document_version_id") as "document_version_ids",
        count("*") as "count"
      )
        .filter($"count" < (documentNumber / 20))
        .select(explode($"document_version_ids") as "document_version_id", $"entity_id")
        .repartition($"document_version_id")
    }

    //get entities count
    val entitiesCountPerDocument = safeData
      .groupBy("document_version_id")
      .agg(
        count("entity_id") as "document_entity_count",
        collect_list($"entity_id") as "entity_ids"
      )
      .select( $"document_version_id",$"document_entity_count",  explode($"entity_ids") as "entity_id")

    //get term frequency
    var entitiesWithTF = entitiesCountPerDocument.groupBy("entity_id", "document_version_id", "document_entity_count").count()
      .withColumn("data", struct($"document_version_id", $"count" as "document_tf", $"document_entity_count"))
      .select("entity_id", "document_version_id", "data")

    //index entities, sort index to avoid duplicated combination, compute idf
    var entitiesIndex = entitiesWithTF
      .groupBy("entity_id")
      .agg(sort_array(collect_list("data")) as "data")
      .withColumn("idf", log10(lit(documentNumber) / size($"data")))
      .cache()

    //prepare similarities genetartion
    val rightData = entitiesIndex.select($"entity_id" as "right_entity_id", explode($"data") as "right_data", $"idf")
    val leftData = entitiesIndex.select($"entity_id" as "left_entity_id", explode($"data") as "left_data")

    //generate similarities
    val similarities = rightData.join(leftData, $"right_entity_id" === $"left_entity_id")
      .filter($"right_data.document_version_id" < $"left_data.document_version_id")
      .select(
        $"right_data.document_version_id" as "document_version1_id",
        $"left_data.document_version_id" as "document_version2_id",
        $"right_data.document_tf" as "document_version1_tf",
        $"left_data.document_tf" as "document_version2_tf",
        $"right_data.document_entity_count" as "document_version1_entity_count",
        $"left_data.document_entity_count" as "document_version2_entity_count",
        $"left_entity_id" as "entity_id",
        $"idf",
        lit(entityType.stringValue) as "score_type"
      )
      .distinct()

    similarities
  }

  override def name: String = "Entities matcher"

  override def validInputType: List[DataFrameType] = EntitiesIdDfType :: Nil

  override def maxInputNumber: Int = 1
}
