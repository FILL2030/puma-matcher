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
package eu.ill.puma.sparkmatcher.matching.analyser

import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.datasource.FileDataSource
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.nlp.CodeAnalyser.CodeAnalyserService
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ListBuffer

class DoiAnalyser(var fileDataSource: FileDataSource,
                  var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                  var dbProperties: Properties = ProgramConfig.dbProperties) extends Analyser {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //input
    val ownedDoi = input.head._2.withColumn("type", lit(DoiType.stringValue))

    val (fileDataFrameType, fileDataFrame) = fileDataSource.loadData

    //analysis
    val analysedDoi = this.analyse(fileDataFrame, ownedDoi)

    (EntitiesIdDfType, analysedDoi)
  }


  def analyse(fileData: DataFrame, ownedProposalCode: DataFrame): DataFrame = {
    import fileData.sparkSession.implicits._

    import scala.collection.JavaConversions._

    val fileDataSet = fileData.select($"document_version_id".as[Long], $"text".as[String])

    //analyse words
    val analyzedWords = fileDataSet.flatMap(document => {
      var list = new ListBuffer[(Long, String, String)]
      val analyzed = CodeAnalyserService.analyseILLDoi(document._2)

      analyzed.foreach(word => {
        list.append((document._1, word, ReferencedDoiType.stringValue))
      })

      list
    }).toDF("document_version_id", "entity", "type").cache()
    
    //generate formula id
    val indexer = new StringIndexer()
      .setInputCol("entity")
      .setOutputCol("entity_id")

    val idModel = indexer.fit(analyzedWords union ownedProposalCode)

    var referencedDoiWithId = idModel.transform(analyzedWords)
    val ownedDoiWithId = idModel.transform(ownedProposalCode)

    //drop referenced element present in ownedProposalCodeWithId
    referencedDoiWithId = ownedDoiWithId.withColumnRenamed("type", "right_type")
      .join(referencedDoiWithId, Seq("document_version_id", "entity", "entity_id"), "right_outer")
      .filter($"right_type".isNull)
      .drop($"right_type")

    //result
    val doi = referencedDoiWithId.select("document_version_id", "entity", "entity_id", "type")
      .union(ownedDoiWithId.select("document_version_id", "entity", "entity_id", "type"))

    //save code to db
    val forDb = doi.groupBy($"document_version_id", $"entity", $"entity_id", $"type").agg(count("*"))
      .select($"entity_id".cast(LongType), $"entity" as "doi", $"document_version_id", $"type", $"count(1)" as "tf")

    forDb.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "doi", dbProperties)

    val forMatcher = doi.select($"entity_id".cast(LongType), $"document_version_id", $"type")

    //return
    forMatcher
  }

  override def name: String = "doi analyser"

  override def validInputType: List[DataFrameType] = TextDfType :: Nil

  override def maxInputNumber: Int = 1
}
