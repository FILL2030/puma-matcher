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

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.nlp.FormulaAnalyser.entities.WordType
import eu.ill.puma.sparkmatcher.utils.nlp.FormulaAnalyser.service.FormulaAnalyserService
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ListBuffer

class FormulaAnalyser(var documentAddressDataSource: DataSource,
                      var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                      var dbProperties: Properties = ProgramConfig.dbProperties) extends Analyser {
  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (EntitiesIdDfType, this.analyse(input.head._2))
  }

  def analyse(data: DataFrame): DataFrame = {
    import data.sparkSession.implicits._

    import scala.collection.JavaConversions._

    val documentAddressDataFrame = documentAddressDataSource.loadData._2

    val fileDataSet = data
      .join(documentAddressDataFrame, Seq("document_version_id"))
      .select($"document_version_id".as[Long], $"text".as[String], $"address".as[String])

    //analyse words
    val analyzedWords = fileDataSet.flatMap(document => {
      var list = new ListBuffer[(Long, String, String)]
      val analyzedWord = FormulaAnalyserService.analyse(document._2, document._3)

      analyzedWord.foreach(word => {
        word.getTypes.foreach(wordType => {
          list.append((document._1, word.getCleanWord, wordType.toString))
        })
      })

      list
    }).toDF("document_version_id", "word", "type")

    //extract formula
    val formula = analyzedWords.filter($"type" === WordType.formula.toString).select($"document_version_id", $"word" as "entity")

    //generate formula id
    val indexer = new StringIndexer()
      .setInputCol("entity")
      .setOutputCol("entity_id")

    val analysedFormula = indexer.fit(formula).transform(formula).cache()

    //group formula and add TF
    val formulaForDb = analysedFormula.groupBy($"document_version_id", $"entity", $"entity_id").agg(count("*"))
      .select($"entity_id".cast(LongType), $"entity" as "formula_code", $"document_version_id", $"count(1)" as "tf")

    //save to db
    formulaForDb.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "formula", dbProperties)

    //return to matcher
    analysedFormula.select($"entity_id".cast(LongType), $"document_version_id")
  }

  override def name: String = "formula analyser"

  override def validInputType: List[DataFrameType] = TextDfType :: Nil

  override def maxInputNumber: Int = 1
}
