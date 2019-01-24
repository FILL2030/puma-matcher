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

import com.databricks.spark.corenlp.functions._
import edu.stanford.nlp.simple.Document
import edu.stanford.nlp.util.logging.RedwoodConfiguration
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.async.Async.async
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DocumentWordSpecAnalyser(
                                var minDistance: Double = 7.3,
                                var tableName: String = "document_word_spec",
                                var minFrequency: Long = ProgramConfig.wordSpecMinFrequency,
                                var matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                                var dbProperties: Properties = ProgramConfig.dbProperties) extends Analyser {


  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    //set stanford nlp logger to off
    RedwoodConfiguration.current().clear().apply();

    (DocumentWordSpecDfType, this.analyse(input.head._2))

  }

  def analyse(fileDF: DataFrame): DataFrame = {
    import fileDF.sparkSession.implicits._


    /**
      * english section
      */

    //read word frequency file
    val rawWordFrequency = fileDF.sparkSession.read.text("/remote/puma-nas/spark-ressources/wordFrequency.txt").toDF("value")
      .withColumn("word", split(col("value"), "\t").getItem(0))
      .withColumn("frequency", split(col("value"), "\t").getItem(1).cast(LongType))
      .drop($"value")
      .cache()

    //get document count based on the number of document with the word "the"
    val englishDocumentCount = rawWordFrequency.filter($"word" === "the").head.getAs[Long]("frequency")

    //extract lemma
    val englishLemma = rawWordFrequency.select($"word").distinct()
      .filter(length($"word") > 1)
      .withColumn("lemma", lemma($"word").getItem(0))

    //compute idf
    val englishWordsIDF = rawWordFrequency
      .join(englishLemma, Seq("word"))
      .groupBy($"lemma")
      .agg(sum($"frequency") as "frequency")
      .withColumn("idf", log(lit(englishDocumentCount) / $"frequency"))
      .select($"lemma", $"frequency" as "english_frequency", $"idf" as "english_idf")

    /**
      * PCC section
      */

    //get pcc corpus sentences
    val corpusWord = fileDF.repartition(ProgramConfig.partitionNumber)
      .select(explode(processInputFilesUdf($"document_version_id", $"text")) as "data")
      .select(
        $"data._1" as "document_version_id",
        $"data._2" as "word",
        $"data._3" as "lemma",
        $"data._4" as "pos_tag",
        $"data._5" as "sentence_position",
        $"data._6" as "word_position",
        $"data._7" as "sentence"
      )
      .cache()

    fileDF
      .select(explode(processInputFilesUdf($"document_version_id", $"text")) as "data")
      .select(
        $"data._1" as "document_version_id",
        $"data._2" as "text")
      .write.mode(SaveMode.Overwrite).jdbc(matchingDatabaseUrl, "debug_word_spec_text", dbProperties)


    //count document
    val corpusDocumentCount = fileDF.select($"document_version_id").distinct().count

    //compute idf
    val corpusWordsIdf = corpusWord
      .groupBy($"lemma")
      .agg(countDistinct($"document_version_id") as "frequency")
      .withColumn("idf", log(lit(corpusDocumentCount) / $"frequency"))
      .select($"lemma", $"frequency" as "pcc_frequency", $"idf" as "pcc_idf")
      .filter($"frequency" > minFrequency)

    //apply idf
    val corpusIdf = corpusWord.join(corpusWordsIdf, Seq("lemma"))

    /**
      * comparaison section
      */

    val wordSpec = englishWordsIDF.join(corpusIdf, Seq("lemma") /*, "right"*/)
      //      .filter($"pos_tag".isin("FW", "MD", "NN", "NNS", "NNP", "NNPS", "RP", "PDT", "UH", "VB", "VBD", "VBG", "VBN", "VBP", "VBZ"))
      //      .filter(length($"lemma") >= 4)
      //      .na.fill(0)
      .select($"document_version_id", $"word", $"lemma", $"pos_tag", $"pcc_idf", $"pcc_frequency", $"english_idf", $"english_frequency", abs($"english_idf" - $"pcc_idf") as "distance", $"sentence_position", $"word_position", $"sentence")
      .filter($"distance" >= lit(minDistance))
      .cache()

    /**
      * Document resume section
      */

    val corpusDocumentCharacteristic = wordSpec
      .select($"document_version_id", $"lemma", $"distance")
      .distinct()
      .groupBy($"document_version_id")
      .agg(
        sum($"distance") as "total_distance",
        avg($"distance") as "avg_distance",
        count($"distance") as "count",
        collect_list($"lemma") as "lemma")
      .cache()


    val future1 = async {
      wordSpec.write.mode(SaveMode.Append).jdbc(ProgramConfig.matchingDatabaseUrl, tableName, ProgramConfig.dbProperties)
    }
    val future2 = async {
      corpusDocumentCharacteristic.write.mode(SaveMode.Overwrite).jdbc(ProgramConfig.matchingDatabaseUrl, "document_resume", ProgramConfig.dbProperties)
    }


    Await.result(future1, 3600.second)
    Await.result(future2, 3600.second)

    corpusDocumentCharacteristic
  }


  def processInputFilesUdf = udf((documentVersionId: Long, text: String) => {

    val result = ListBuffer.empty[(Long, String, String, String, Long, Long, List[String])]

    val allowedPosTag = "FW" :: "MD" :: "NN" :: "NNS" :: "NNP" :: "NNPS" :: "RP" :: "PDT" :: "UH" :: "VB" :: "VBD" :: "VBG" :: "VBN" :: "VBP" :: "VBZ" :: Nil

    //clean text
    var cleanText = text
      .replaceAll("\r\n", " ")
      .replaceAll("\n", " ")
      .replaceAll("""[^a-zA-Z0-9;:,.?!\- ]""", " ")

    //nlp document
    val nlpDocument = new Document(cleanText)
    val sentences = nlpDocument.sentences()

    //position
    var wordPosition = 1L
    var sentencePosition = 1L

    import scala.collection.JavaConversions._

    //extract sentence data
    sentences.foreach(sentence => {
      val words = sentence.words()
      val lemma = sentence.lemmas()
      val posTag = sentence.posTags()

      //for each word
      for (i <- 0 until words.size()) {

        //filter
        if (allowedPosTag.contains(posTag(i)) && lemma(i).length >= 4) {

          //add to result
          result.add((
            documentVersionId,
            words(i).toLowerCase,
            lemma(i).toLowerCase,
            posTag(i),
            sentencePosition,
            wordPosition,
            sentence.words().toList.map(_.toLowerCase).filter(_.length > 1).filter(word => !ProgramConfig.stopwords.contains(word))
          ))
        }

        wordPosition += 1
      }

      sentencePosition += 1
    })

    //return
    result
  })

  def processInputFilesUdf2 = udf((documentVersionId: Long, text: String) => {

    val result = ListBuffer.empty[(Long, String)]

    val allowedPosTag = "FW" :: "MD" :: "NN" :: "NNS" :: "NNP" :: "NNPS" :: "RP" :: "PDT" :: "UH" :: "VB" :: "VBD" :: "VBG" :: "VBN" :: "VBP" :: "VBZ" :: Nil

    //clean text
    var cleanText = text
      .replaceAll("\r\n", " ")
      .replaceAll("\n", " ")
      .replaceAll("[^a-zA-Z0-9;:,.?! ]", " ")

    import scala.collection.JavaConversions._

    //add to result
    result.add((
      documentVersionId,
      cleanText
    ))

    //return
    result
  })

  override def name = "document word analyser"

  override def validInputType: List[DataFrameType] = TextDfType :: Nil

  override def maxInputNumber: Int = 1

}
