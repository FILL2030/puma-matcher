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

import edu.stanford.nlp.simple.Sentence
import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.nlp.PorterStemmer
import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode}


class TextEntitiesAnalyser(
                            val documentTypeDataSource: DataSource,
                            val entitiesDataSource: List[DataSource],
                            val rawOutput: Boolean = true,
                            val sentenceWindowSize: Int = 5,
                            val stemmingMinLength: Int = 5,
                            val stemMinLength: Int = 0,
                            val tableName: String = "technique",
                            val saveTechnique: Boolean = true,
                            val partitionNumber: Int = ProgramConfig.partitionNumber,
                            val stopWords: List[String] = ProgramConfig.stopwords,
                            val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                            val dbProperties: Properties = ProgramConfig.dbProperties
                          ) extends Analyser {


  val sparkSession = documentTypeDataSource.loadData._2.sparkSession

  import sparkSession.implicits._

  /**
    * run the analyser
    *
    * @param input      the input dataFrame, usually a text dataframe
    * @param entityType the entity type used
    * @return
    */
  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {

    /**
      * Input dataframe
      */

    //document type
    val typeDataFrame = documentTypeDataSource.loadData._2

    //document text
    val textDataFrame = input.head._2.select($"document_version_id", $"text")

    //technique
    val techniqueWords = entitiesDataSource
      .map(_.loadData._2.select($"tag", $"name", $"tag_id", $"use_stemming").distinct).reduce(_ union _)
      .repartition(typeDataFrame.rdd.getNumPartitions)
      .withColumn("ngram", concat_ws(" ", split(lower($"name"), "\\W+|(?<!^)(?=[A-Z])")))
      .drop("name")

    //document count
    val documentCount = textDataFrame.select($"document_version_id").distinct().count()

    /**
      * Build index
      */

    //filter publication only
    val publicationText = this.filterPublicationOnly(textDataFrame, typeDataFrame)

    //extract text word
    val nGrams = this.extractNGramFromTextData(publicationText)

    //build index
    val index = this.buildIndex(nGrams, documentCount, techniqueWords :: Nil)

    //extract potential features
    val potentialFeaturesWithSentence = this.extractPotentialFeatures(nGrams, index)

    /**
      * save potential result and return
      */

    if (saveTechnique) this.saveTechnicsDataFrame(potentialFeaturesWithSentence)

    if (rawOutput) (TextEntitiesAnalyserDfType, potentialFeaturesWithSentence.repartition($"document_version_id"))
    else (EntitiesIdDfType, potentialFeaturesWithSentence.select($"document_version_id", $"tag_id" as "entity_id").distinct().repartition($"document_version_id"))

  }

  /**
    * keep only the publication in the text given dataframe
    *
    * @param textDataFrame the dataframe to filter
    * @param typeDataFrame the type dataframe foreach document
    * @return
    */
  def filterPublicationOnly(textDataFrame: DataFrame, typeDataFrame: DataFrame): DataFrame = {
    textDataFrame.join(typeDataFrame, Seq("document_version_id"))
      .filter($"document_type".startsWith("PROPOSAL") === false)
      .drop($"document_type")
  }

  /**
    * create the ngram 1 - 5 for the given document dataframe
    *
    * @param textDataFrame the document dataFrame (document_version_id, text)
    * @return the nGram dataframe (document_version_id, ngram_position, ngram, ngram_size)
    */
  def extractNGramFromTextData(textDataFrame: DataFrame): DataFrame = {

    val cleanText = textDataFrame
      .withColumn("text", lower($"text"))
      .withColumn("text", regexp_replace($"text", "\r\n", " "))
      .withColumn("text", regexp_replace($"text", "\n", " "))
      .withColumn("text", regexp_replace($"text", """[^a-zA-Z0-9;:,.?!\- ]""", " "))

    val words = cleanText
      .select($"document_version_id", customStringfilter(split(lower($"text"), "\\W+|(?<!^)(?=[A-Z])")) as "words")
      .cache()

    val ngramTransformer1 = new NGram().setInputCol("words").setOutputCol("ngrams").setN(1)
    val ngramTransformer2 = new NGram().setInputCol("words").setOutputCol("ngrams").setN(2)
    val ngramTransformer3 = new NGram().setInputCol("words").setOutputCol("ngrams").setN(3)
    val ngramTransformer4 = new NGram().setInputCol("words").setOutputCol("ngrams").setN(4)
    val ngramTransformer5 = new NGram().setInputCol("words").setOutputCol("ngrams").setN(5)

    val n1 = ngramTransformer1.transform(words).withColumn("ngram_size", lit(1))
    val n2 = ngramTransformer2.transform(words).withColumn("ngram_size", lit(2))
    val n3 = ngramTransformer3.transform(words).withColumn("ngram_size", lit(3))
    val n4 = ngramTransformer4.transform(words).withColumn("ngram_size", lit(4))
    val n5 = ngramTransformer5.transform(words).withColumn("ngram_size", lit(5))

    val result = (n1 union n2 union n3 union n4 union n5).select($"document_version_id", posexplode($"ngrams") as Seq("ngram_position", "ngram"), $"ngram_size")

    result
  }

  /**
    * index each nGram and compute their ML data like idf
    *
    * @param nGrams        the Ngrams which contains the dataFrame (document_version_id, ngram_position, ngram, ngram_size)
    * @param documentCount the number of document present in the corpus
    * @param tagWord       the word to tag in the index as technique, instrument, formula etc (tag_id, ngram, tag, use_stemming)
    * @return the index
    */
  def buildIndex(nGrams: DataFrame, documentCount: Long, tagWord: List[DataFrame]): DataFrame = {

    /**
      * Build index
      */

    //build index foreach ngram
    val textWord = nGrams
      .repartition(this.partitionNumber, $"ngram")
      .groupBy($"ngram", $"ngram_size")
      .agg(collect_list(struct($"document_version_id", $"ngram_position")) as "index_data")
      .withColumn("ngram_length", length($"ngram"))
      .withColumn("stem", when($"ngram_length" > stemmingMinLength, stem($"ngram")).otherwise($"ngram"))
      .withColumn("ngram_index", monotonically_increasing_id())
      .cache()

    //compute idf for each stem
    val textStem = textWord
      .select($"stem", $"index_data.document_version_id")
      .groupBy($"stem")
      .agg(countDistinct($"document_version_id") as "ngram_frequency")
      .withColumn("idf", log10(lit(documentCount) / $"ngram_frequency"))
      .withColumn("stem_index", monotonically_increasing_id())

    val index = textStem.join(textWord, Seq("stem")).cache

    /**
      * tag index
      */

    //prepare words tag
    val tag = tagWord
      .map(_.select($"tag", $"ngram", $"tag_id", $"use_stemming").distinct).reduce(_ union _)
      .withColumn("stem", when($"use_stemming" and length($"ngram") > stemmingMinLength, stem($"ngram")).otherwise($"ngram"))
      .select($"tag", $"tag_id", $"stem")
      .cache()

    //join index and tag
    val indexWithTag = index
      .join(tag, Seq("stem"), "left")

    indexWithTag
  }

  /**
    * extract potential feature from the index
    *
    * @param nGrams the ngram datafram, used to get the sentences close to the feature(document_version_id, ngram_position, ngram, ngram_size)
    * @param index  the index dataframe
    * @return the potential feature dataframe
    */
  def extractPotentialFeatures(nGrams: DataFrame, index: DataFrame): DataFrame = {

    //get potential feature
    val potentialFeatures = index
      .filter($"tag".isNotNull)
      .select($"stem", $"idf", $"stem_index", $"ngram" as "index_ngram", $"ngram_size", $"ngram_frequency", explode($"index_data") as "index_data", $"ngram_length", $"ngram_index", $"tag", $"tag_id")

    //keep only single words (ngram 1)
    val words = nGrams
      .filter($"ngram_size" === 1)
      .drop("ngram_size")

    //extract sentences
    val potentialFeaturesWithSentence = potentialFeatures.join(words,
      $"document_version_id" === $"index_data.document_version_id" &&
        $"ngram_position" <= $"index_data.ngram_position" + sentenceWindowSize &&
        $"ngram_position" >= $"index_data.ngram_position" - sentenceWindowSize
    )
      .orderBy($"document_version_id", $"index_data.ngram_position", $"ngram_position")
      .groupBy(
        $"document_version_id",
        $"index_data.ngram_position" as "feature_position",
        $"stem" as "feature_stem",
        $"index_ngram" as "feature_ngram",
        $"ngram_size" as "feature_size",
        $"idf" as "feature_idf",
        $"ngram_frequency" as "feature_frequency",
        $"tag",
        $"tag_id",
        $"ngram_index"
      )
      .agg(
        collect_list($"ngram") as "sentence"
      )

    potentialFeaturesWithSentence
  }

  /**
    * save the given feature dataframe
    *
    * @param potentialFeature
    */
  def saveTechnicsDataFrame(potentialFeature: DataFrame) = {

    potentialFeature
      .select(
        ($"document_version_id" * 10000000L + $"tag_id").cast(StringType) as "row_id",
        $"document_version_id",
        $"tag_id" as s"technique_id",
        $"feature_position",
        lit(0) as "prediction",
        lit(0) as "score",
        array($"feature_ngram") as "technique_name",
        customReplace($"feature_ngram", concat_ws(" ", $"sentence")) as "sentence"
      )
      .write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, tableName, dbProperties)
  }


  def stem: UserDefinedFunction = udf((word: String) => {
    if (word != null && !word.contains(" ")) PorterStemmer.stem(word)
    else if (word.contains(" ")) PorterStemmer.stem(word.split(" ")).mkString(" ")
    else null
  })

  def pos: UserDefinedFunction = udf((text: String) => if (text != null && text.length > 0) new Sentence(text).posTag(0) else "")

  def customStringfilter: UserDefinedFunction = udf((words: Seq[String]) => words.filter(_.nonEmpty))

  def sumArray: UserDefinedFunction = udf((numbers: Seq[Double]) => numbers.sum)

  def customReplace: UserDefinedFunction = udf((instrument: String, sentence: String) => sentence.replace(instrument, "<b>" + instrument + "</b>"))

  override def name: String = "text entities analyser"

  override def maxInputNumber: Int = 1

  override def validInputType: List[DataFrameType] = TextDfType :: Nil
}


//def prepareText(textWords: DataFrame, ngramIndex: DataFrame): DataFrame = {
//  RedwoodConfiguration.current().clear().apply();
//
//  var ngramData = textWords
//
/**
  * Pos tag
  */

//    //load cached pos tag from db
//    val posTagFromDb = wordPosTagDataSource.loadData._2
//
//    //compute pos tag
//    val posTag = ngramData.select($"ngram").distinct
//      .join(posTagFromDb, Seq("word"), "left")
//      .withColumn("pos_tag", when($"pos_tag".isNull, pos($"word")).otherwise($"pos_tag"))
//      .cache
//
//    val posTagIndex = broadcast(
//      posTag
//        .select($"pos_tag").distinct()
//        .withColumn("pos_tag_index", row_number().over(Window.orderBy($"pos_tag")))
//    )
//
//    val posTagWithIndex = posTag
//      .join(posTagIndex, Seq("pos_tag"))
//      .select($"word", $"pos_tag_index")
//
//    //cache pos tag for next run
//    posTag
//      .join(posTagFromDb, Seq("word", "pos_tag"), "leftanti")
//      .write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, "word_pos_tag", dbProperties)
//
//    //apply pos tag
//    ngramData = ngramData
//      .join(posTagWithIndex, Seq("word"))

/**
  * Index
  */


//apply index
//    ngramData = ngramData
//      .join(ngramIndex, Seq("ngram"))
//      .drop("ngram", "stem")

/**
  * TF
  */

//    //compute tf
//    val tf = ngramData
//      .groupBy($"document_version_id", $"stem_index")
//      .agg(count($"ngram_position") as "tf")
//
//    //apply tf and idf
//    ngramData = ngramData
//      .join(tf, Seq("document_version_id", "stem_index"))

//    (ngramData.cache, posTagIndex.cache())
// ngramData.cache
//}