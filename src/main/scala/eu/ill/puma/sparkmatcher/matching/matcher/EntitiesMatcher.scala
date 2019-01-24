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
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel

class EntitiesMatcher(
                       var maximumEntitiesOccurency: Int = ProgramConfig.maximumEntitiesOccurency,
                       var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                       var dbProperties: Properties = ProgramConfig.dbProperties) extends Matcher {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (EntitySimilarityDfType, this.matchData(input.head._2, entityType))
  }

  def matchData(data: DataFrame, entityType: EntityType): DataFrame = {
    import data.sparkSession.implicits._

    //get entities count
    val entitiesCountPerDocument = data.groupBy("document_version_id").agg(count("entity_id") as "entity_count").join(data, "document_version_id")

    //get term frequency
    var TF = entitiesCountPerDocument.groupBy("entity_id", "document_version_id", "entity_count").count()
      .withColumn("TF", struct($"document_version_id", $"count", $"entity_count")).select("entity_id", "document_version_id", "TF")

    //index entities and sort index to avoid duplicated combination
    var entitiesIndex = TF.groupBy("entity_id").agg(sort_array(collect_list("TF")) as "TF").cache()

    //compute IDF
    entitiesIndex = entitiesIndex.withColumn("IDF", log10(lit(entitiesCountPerDocument.count) / size($"TF")))

    //convert to dataSet
    val entitiesIndexDS = entitiesIndex.select($"entity_id".as[Long], $"TF".as[Seq[(Long, Long, Long)]], $"IDF".as[Double]).cache()

    //filter entity id with too many occurrences
    val filteredEntitiesIndexDS = entitiesIndexDS.filter(row => row._2.size <= maximumEntitiesOccurency)

    //save entities with to many occurences to DB
    entitiesIndexDS.filter(row => row._2.size > maximumEntitiesOccurency).select("entity_id").withColumn("type", lit(entityType.stringValue)).write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "excluded_entities", dbProperties)

    //create similarities row._1 = entity_id
    val similarities = filteredEntitiesIndexDS.flatMap(row => (row._2.combinations(2).map(sim => (sim(0)._1, sim(1)._1, sim(0)._2, sim(1)._2, sim(0)._3, sim(1)._3, row._1, row._3, entityType.stringValue))))

    //convert to dataFrame
    val similaritiesDF = similarities.toDF("document_version1_id", "document_version2_id", "document_version1_tf", "document_version2_tf", "document_version1_entity_count", "document_version2_entity_count", "entity_id", "idf", "score_type")

    //return
    similaritiesDF
  }

  override def name: String = "Entities matcher"

  override def validInputType: List[DataFrameType] = EntitiesIdDfType :: Nil

  override def maxInputNumber: Int = 1
}
