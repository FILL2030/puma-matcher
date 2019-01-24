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

import eu.ill.puma.sparkmatcher.matching.pipepline.{AdvTextDfType, DataFrameType, EntityType, TextSimilarityDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{collect_list, collect_set, size}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TextMatcher(var sentenceMinLength: Int = ProgramConfig.sentenceLength,
                  var maximumSentenceOccurency: Int = ProgramConfig.maximumSentenceOccurency) extends Matcher {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //analysis
    (TextSimilarityDfType, this.matchData(input.head._2))
  }

  def matchData(data: DataFrame): DataFrame = {
    import data.sparkSession.implicits._

    //word data set
    val wordsDs: Dataset[(Long, Array[String])] = data.na.fill("").select($"document_version_id".as[Long], $"feature".as[Array[String]])

    //generate ngrams
    val nGramDataSet: Dataset[(Long, Array[Array[String]])] = wordsDs.map(x => (x._1, x._2.sliding(sentenceMinLength).toArray))

    //indexNgrams
    val flatNGramDataSetWithPos = nGramDataSet.flatMap(document => {
      document._2.map(sentence => ((document._1, document._2.indexOf(sentence)), sentence))
    })
      .cache()

    val index = flatNGramDataSetWithPos.groupBy("_2").agg(collect_set("_1") as "document_version_ids").filter(row => row.getSeq(1).size > 1 && row.getSeq(1).size < maximumSentenceOccurency)
      .withColumnRenamed("_2", "index_key")

    //generateSimilarities
    val indexDataSet = index.select($"index_key".as[Seq[String]], $"document_version_ids".as[Seq[(Long, Long)]]).map(row => (row._1, row._2.sortBy(_._1)))

    val similarities = indexDataSet.flatMap(row => (row._2.combinations(2).map(sim => (sim(0)._1, sim(0)._2, sim(1)._1, sim(1)._2, sentenceMinLength, row._1))))
      .toDF("document_version1_id", "doc1_sentence_position", "document_version2_id", "doc2_sentence_position", "sentences_size", "sentence").filter($"document_version1_id" =!= $"document_version2_id")

    //groupTextSimilarities
    val temp = similarities.orderBy($"document_version1_id", $"document_version2_id", $"doc1_sentence_position")
      .groupBy("document_version1_id", "document_version2_id", "sentences_size")
      .agg(collect_list("doc1_sentence_position") as "document_version1_position", collect_list("doc2_sentence_position") as "document_version2_position", collect_list("sentence") as "sentence")
      .orderBy($"document_version1_id", $"document_version2_id", $"sentences_size")

    val outputEncoder = RowEncoder.apply(StructType(
      Array(
        StructField("document_version1_id", LongType, nullable = false),
        StructField("document_version2_id", LongType, nullable = false),
        StructField("doc1_sentence_position", ArrayType(LongType)),
        StructField("doc2_sentence_position", ArrayType(LongType)),
        StructField("sentences_size", ArrayType(LongType)),
        StructField("max_sentence_size", LongType, nullable = false),
        StructField("text_match_count", LongType, nullable = false),
        StructField("word_match_count", LongType, nullable = false)
      )
    ))

    val groupedSimilarities = temp.flatMap(groupSentenceFMF, outputEncoder)

    //word count
    val wordCount = nGramDataSet.select($"_1" as "document_version_id", size($"_2") as "word_count").groupBy($"document_version_id").sum("word_count").withColumnRenamed("sum(word_count)", "word_count")

    //merge
    val ret = groupedSimilarities.join(wordCount, $"document_version_id" === $"document_version1_id", "left").withColumnRenamed("word_count", "doc1_word_count").drop("document_version_id")
      .join(wordCount, $"document_version_id" === $"document_version2_id", "left").withColumnRenamed("word_count", "doc2_word_count").drop("document_version_id")

    ret
  }

  def groupSentenceFMF = new FlatMapFunction[Row, Row]() {
    override def call(row: Row): util.Iterator[Row] = {
      val rows = new util.ArrayList[Row]()

      //values from rows
      val sentenceSize = row.getInt(2).toLong
      val doc1Positions = row.getAs[mutable.WrappedArray[Long]](3)
      val doc2Positions = row.getAs[mutable.WrappedArray[Long]](4)
      val sentences = row.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]](5)

      if (sentences.size < 10) {
        rows.iterator()
      }

      //check data coherency
      if (doc1Positions.length == doc2Positions.length && doc2Positions.length == sentences.length) {

        //new values arrays
        val newsDoc1Positions = ListBuffer.empty[java.lang.Long]
        val newsDoc2Positions = ListBuffer.empty[java.lang.Long]
        val newsSentences = ListBuffer.empty[ListBuffer[String]]
        val newsSentencesSize = ListBuffer.empty[Long]

        //current values
        var currentSentenceLength: Long = sentenceSize

        //loop over each sentence of the row
        for (cursor <- doc1Positions.indices) {
          //contigous sentence
          if (cursor.toInt > 0
            && doc1Positions(cursor.toInt - 1) + sentenceSize >= doc1Positions(cursor.toInt)
            && doc2Positions(cursor.toInt - 1) + sentenceSize >= doc2Positions(cursor.toInt)) {

            //update sentence length
            val step = doc1Positions(cursor.toInt) - doc1Positions(cursor.toInt - 1)
            currentSentenceLength += step

            //append new words to sentence
            val currentSentence = ListBuffer(sentences(cursor.toInt): _ *)
            val previousSentence = newsSentences.last
            newsSentences(newsSentences.length - 1) = previousSentence ++= currentSentence.slice(sentenceSize.toInt - step.toInt, sentenceSize.toInt)

          } else {
            // init or start a new sentence
            newsDoc1Positions += doc1Positions(cursor.toInt)
            newsDoc2Positions += doc2Positions(cursor.toInt)

            currentSentenceLength = sentenceSize

            newsSentences += ListBuffer(sentences(cursor.toInt): _ *)
          }
        }

        //senetence size
        for (sentence <- newsSentences) {
          newsSentencesSize += sentence.size.toLong
        }

        rows.add(Row.fromTuple((row.getLong(0), row.getLong(1), newsDoc1Positions, newsDoc2Positions, newsSentencesSize, newsSentencesSize.reduceLeft(_ max _).toLong, newsSentencesSize.length.toLong, newsSentencesSize.sum)))
      }
      rows.iterator
    }
  }

  override def name: String = "Text matcher"

  override def validInputType: List[DataFrameType] = AdvTextDfType :: Nil

  override def maxInputNumber: Int = 1
}
