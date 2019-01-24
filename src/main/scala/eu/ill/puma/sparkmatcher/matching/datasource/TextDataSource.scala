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
package eu.ill.puma.sparkmatcher.matching.datasource

import eu.ill.puma.sparkmatcher.matching.pipepline.{AdvTextDfType, DataFrameType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.nlp.PorterStemmer
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

class TextDataSource(dataSource: DataSource, var name: String,
                     val minimumWordLength : Int = ProgramConfig.minimumWordLength,
                     val stopWords : List[String] = ProgramConfig.stopwords) extends DataSource {

  private var fileDataFrame: Option[DataFrame] = None

  override def loadData: (DataFrameType, DataFrame) = {
    var schema = StructType(
      Array(
        StructField("document_version_id", LongType, nullable = false),
        StructField("feature", ArrayType(StringType), nullable = false),
        StructField("position", ArrayType(LongType), nullable = false),
        StructField("words", ArrayType(StringType), nullable = false)
      )
    )

    def processInputDataMapFunction = new MapFunction[Row, Row]() {
      override def call(row: Row): Row = {
        //input data
        val text = row.getAs[String]("text").toLowerCase
        val documentId = row.getAs[Long]("document_version_id")

        //word spilt
        val words = text.split("\\W+").filter(_.size >= minimumWordLength)

        //stop word removal
        val (filteredWords, positions) = words.zipWithIndex
          .filter(_._1.length > 1)
          .filter(word => !stopWords.contains(word._1)).unzip

        //stemming
        val stemFeature = PorterStemmer.stem(filteredWords)

        //output and ngrams
        Row.fromTuple((documentId, stemFeature, positions, filteredWords))
      }
    }

    if (this.fileDataFrame.isEmpty) {
      this.fileDataFrame = Some(dataSource.loadData._2.map(processInputDataMapFunction, RowEncoder.apply(schema)).cache())
    }


    (AdvTextDfType, this.fileDataFrame.get)
  }
}
