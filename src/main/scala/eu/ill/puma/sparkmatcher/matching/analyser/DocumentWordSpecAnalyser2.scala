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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class DocumentWordSpecAnalyser2(
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

    //count corpus document
    val corpusDocumentCount = fileDF.select($"document_version_id").distinct().count

    val englishDocumentCountNormalisationFactor = englishDocumentCount / corpusDocumentCount

    println(corpusDocumentCount)
    println(englishDocumentCount)
    println(englishDocumentCountNormalisationFactor)

    //extract lemma
    val englishLemma = rawWordFrequency.select($"word").distinct()
      .filter(length($"word") > 1)
      .withColumn("lemma", lemma($"word").getItem(0))

    //compute idf
    val englishWordsIDF = rawWordFrequency
      .join(englishLemma, Seq("word"))
      .groupBy($"lemma")
      .agg(sum($"frequency") as "english_raw_frequency")
      .withColumn("frequency", $"english_raw_frequency" / englishDocumentCountNormalisationFactor)
      .withColumn("idf", log(lit(corpusDocumentCount) / $"frequency"))
      .select($"lemma", $"frequency" as "english_frequency", $"english_raw_frequency", $"idf" as "english_idf")

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
        //        $"data._7" as "sentence",
        $"data._7" as "document_length"
        //        size($"data._7") as "sentence_length"
      )
      .cache()


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

    val wordSpec = corpusIdf.join(englishWordsIDF, Seq("lemma") /*, "right"*/)
      .select(
        $"document_version_id",
        $"word", $"lemma",
        $"pos_tag",
        $"pcc_idf",
        $"pcc_frequency",
        $"english_idf",
        $"english_frequency",
        $"english_raw_frequency",
        $"english_idf" - $"pcc_idf" as "distance",
        $"sentence_position",
        $"word_position" / $"document_length" as "word_position_percent",
        $"word_position",
        $"document_length"
        //      $"sentence_length",
        //      $"sentence"
      )
      .filter($"distance" >= lit(minDistance))
      .filter($"word_position_percent" <= lit(0.33))
      .cache()

    wordSpec
      .orderBy($"distance".desc)
      .select($"lemma", $"pcc_idf", $"english_idf", $"distance").distinct()
      //      .drop("sentence")
      .show(200, true)


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

    //    corpusDocumentCharacteristic.show(true)

    //    val future1 = async {
    //      wordSpec
    //        .select($"document_version_id", $"word", $"lemma", $"pos_tag", $"pcc_idf", $"pcc_frequency", $"english_idf", $"english_frequency", abs($"english_idf" - $"pcc_idf") as "distance", $"sentence_position", $"word_position", $"sentence")
    //        .write.mode(SaveMode.Append).jdbc(ProgramConfig.matchingDatabaseUrl, tableName, ProgramConfig.dbProperties)
    //    }
    //    val future2 = async {
    //      corpusDocumentCharacteristic.write.mode(SaveMode.Overwrite).jdbc(ProgramConfig.matchingDatabaseUrl, "document_resume", ProgramConfig.dbProperties)
    //    }
    //
    //
    //    Await.result(future1, 3600.second)
    //    Await.result(future2, 3600.second)
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
    result.map(x => (x._1, x._2, x._3, x._4, x._5, x._6, /*x._7,*/ wordPosition))
  })

  override def name = "document word analyser"

  override def validInputType: List[DataFrameType] = TextDfType :: Nil

  override def maxInputNumber: Int = 1

}


object RAKE {
  private val sentenceDelimiter = """[\\[\\]\n.!?,;:\t\\-\\"\\(\\)\\\'\u2019\u2013]""".r
  private val maxWordsInPhrase: Int = 2
  private val minCharLength: Int = 1
  private val stopWords = ProgramConfig.stopwords.toSet


  private def splitTextToSentences(text: String): List[String] = {
    sentenceDelimiter.split(text).toList
  }

  private def getPhrasesForSentence(listOfWords: List[String])(predicate: String => Boolean): List[List[String]] = {
    listOfWords match {
      case Nil => Nil
      case x :: xs =>
        val phrase = listOfWords takeWhile predicate
        if (phrase.isEmpty || phrase.length > maxWordsInPhrase) {
          getPhrasesForSentence(listOfWords.drop(1))(predicate)
        }
        else {
          phrase :: getPhrasesForSentence(listOfWords.drop(phrase.length + 1))(predicate)
        }
    }
  }

  private def isAcceptableString(word: String): Boolean = {
    if (word.length < minCharLength) return false
    if (!isAplha(word)) return false
    if (stopWords.contains(word)) return false
    return true
  }

  private def isAplha(word: String): Boolean = {
    word.matches("[a-z]+")
  }


  private def generateCandidateKeywords(sentences: List[String]): List[List[String]] = {
    val splittedSentences: List[List[String]] = sentences.map(sentence => sentence.trim.split("\\s+").map(_.toLowerCase).toList)
    splittedSentences.flatMap(sentenceList => getPhrasesForSentence(sentenceList)(isAcceptableString))
  }

  private def calculateWordScores(listOfPhrases: List[List[String]]): Map[String, Double] = {
    val completeListOfWords = listOfPhrases.flatten
    val wordFreqList: List[(String, Int)] = completeListOfWords.map(word => (word, 1))
    val wordDegreeListPartial: List[(String, Int)] = listOfPhrases.flatMap { phrase =>
      val degree = phrase.length - 1
      phrase.map(word => (word, degree))
    }
    val wordFreqMap: Map[String, Int] = wordFreqList.groupBy(k => k._1).mapValues(l => l.map(_._2).sum)
    val wordDegreeMap: Map[String, Int] = (wordFreqList ++ wordDegreeListPartial).groupBy(k => k._1).mapValues(l => l.map(_._2).sum)
    wordFreqMap.keys.map(word => word -> wordDegreeMap(word).toDouble / wordFreqMap(word)).toMap
  }

  private def calculateScoresForPhrase(listOfPhrases: List[List[String]], wordScores: Map[String, Double]) = {
    listOfPhrases.map(phrase => phrase.mkString(" ") -> phrase.map(wordScores(_)).sum).toMap
  }

  def run(sentences: List[String]): List[(String, Double)] = {
    val listOfPhrases = generateCandidateKeywords(sentences)
    //    println(listOfPhrases)
    val wordScores = calculateWordScores(listOfPhrases)
    //    println(wordScores)
    val phraseScores = calculateScoresForPhrase(listOfPhrases, wordScores)
    //    println(phraseScores)
    val orderedPhrases = phraseScores.toList.sortBy(x => x._2).reverse
    //    println(orderedPhrases)
    orderedPhrases
  }

}
