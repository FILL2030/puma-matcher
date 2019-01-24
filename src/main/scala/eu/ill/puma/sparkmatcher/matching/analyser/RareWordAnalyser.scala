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

import eu.ill.puma.sparkmatcher.matching.pipepline.{AdvTextDfType, DataFrameType, EntityType, PictureHashDfType}
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

class RareWordAnalyser(var maximumRareWordCount: Int, var minimumRareWordTf: Int) extends Analyser {
  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (PictureHashDfType, this.analyse(input.head._2))
  }

  def analyse(data: DataFrame): DataFrame = {
    import data.sparkSession.implicits._

    val words = data.select($"document_version_id", explode($"feature") as "word")

    val wordTf = words.groupBy($"word", $"document_version_id").agg(count("*") as "tf")

    val wordCount = words.groupBy($"word").agg(count("*") as "word_document_count")

    val rareWords = words.join(wordCount, Seq("word")).join(wordTf, Seq("word", "document_version_id"))

    def rareWordFilterFunction = new FilterFunction[Row] {
      override def call(row: Row): Boolean = {
        val word = row.getAs[String]("word")
        val documentWordCount = row.getAs[Long]("word_document_count")
        val tf = row.getAs[Long]("tf")

        if (tf < minimumRareWordTf) return false
        if (documentWordCount < 2) return false
        if (documentWordCount > maximumRareWordCount) return false
        if (word.length < 5) return false
        if (word.length > 90) return false
        if (!word.forall(_.isLetter)) return false

        return true
      }
    }

    val filteredWords = rareWords.filter(rareWordFilterFunction)

    //generate formula id
    val indexer = new StringIndexer()
      .setInputCol("word")
      .setOutputCol("entity_id")

    val indexedRareWord = indexer.fit(filteredWords).transform(filteredWords).cache()

    val analyzedRareWord = indexedRareWord.select($"entity_id".cast(LongType), $"document_version_id", $"word" as "entity", $"tf")

    analyzedRareWord
  }

  def setMaximumRareWordCount(value: Double) = {
    maximumRareWordCount = value.toInt
  }

  def setMinimumRareWordTf(value: Double) = {
    minimumRareWordTf = value.toInt
  }

  override def name: String = "Rare word analyser"

  override def validInputType: List[DataFrameType] = AdvTextDfType :: Nil

  override def maxInputNumber: Int = 1
}
