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
import edu.stanford.nlp.util.logging.RedwoodConfiguration
import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, EntitiesIdDfType, EntityType, TextDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class AdvancedInstrumentAnalyser(val instrumentDataSource: DataSource,
                                 val instrumentNameDataSource: DataSource,
                                 val instrumentAliasDataSource: DataSource,
                                 val typeDataSource: DataSource,
                                 val trainingDataDataSource: DataSource,
                                 val tableName: String = "analysed_instrument",
                                 val saveEntities: Boolean = true,
                                 val wordDistanceFromInstrument: Long = 15,
                                 val minimumTextWordSize: Long = 1,
                                 val minimumModelWordSize: Long = 2,
                                 val trainingDataFrameRatio: Double = 0.7,
                                 val testingDataFrameRatio: Double = 0.3,
                                 val wordModelMinProbality: Double = 0.01,
                                 val perInstrumentWordModelMinCount: Long = 2,
                                 val perInstrumentWordModelMaxRank: Long = 20,
                                 val partitionNumber: Int = ProgramConfig.partitionNumber,
                                 val maximumInstrumentOccurencies: Double = ProgramConfig.maximumInstrumentFrequency,
                                 val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                                 val stopWords: List[String] = ProgramConfig.stopwords,
                                 val dbProperties: Properties = ProgramConfig.dbProperties) extends Analyser {

  override def run(input: List[(DataFrameType, DataFrame)], entityType: EntityType): (DataFrameType, DataFrame) = {


    //set stanford nlp logger to off
    RedwoodConfiguration.current().clear().apply();

    //init
    val textDf = input.head._2
    val corpusInstrumentDataFrame = instrumentDataSource.loadData._2
    val instrumentNameDataFrame = instrumentNameDataSource.loadData._2
    val instrumentAliasDataFrame = instrumentAliasDataSource.loadData._2
    val documentTypeDataFrame = typeDataSource.loadData._2
    val documentCount = textDf.select("document_version_id").distinct().count()
    val instrumentTrainingData = trainingDataDataSource.loadData._2.filter(col("training_group") === "instrument")

    import textDf.sparkSession.implicits._

    //prepare and format proposal instrument
    val proposalInstrument = corpusInstrumentDataFrame
      .join(documentTypeDataFrame, Seq("document_version_id"))
      .filter($"document_type".startsWith("PROPOSAL"))
      .withColumn("row_id", ($"document_version_id" * 10000000L + $"entity_id").cast(StringType))
      .select($"entity_id", $"row_id", $"document_version_id", array($"code") as "instrument_name", lit(false) as "duplicated_code", lit("") as "instrument_sentence", lit(100) as "instrument_confidence", lit(0) as "max_score", lit(1) as "prediction")

    //prepare static instrument data
    val instrumentNameDf = this.prepareInstrumentName(instrumentNameDataFrame, instrumentAliasDataFrame)
    val instrumentCodeList = instrumentNameDf.select($"instrument_code").distinct().collect().map(_.getAs[String]("instrument_code"))

    //prepare text
    val wordsDataFrame = this.prepareDocumentText(textDf, documentTypeDataFrame)

    //find document instrument
    var analysedInstrument = this.findInstrumentFromText(wordsDataFrame, instrumentNameDf)

    analysedInstrument = this.markTrainingData(analysedInstrument, instrumentTrainingData)

    //build statistic model
    val wordModel = this.buildWordModel(analysedInstrument, instrumentNameDf)

    //build statistic model per instrument
    val perInstrumentWordModel = this.buildWordModelPerInstrument(analysedInstrument, instrumentNameDf)

    //score instrument from word
    val scores = this.scoreInstrumentCandidate(wordModel, perInstrumentWordModel, analysedInstrument)

    //filter instrument with too many occurrences
    val filteredScore = this.filterInstrumentWithTooManyOccurencies(scores, documentCount)

    //group by document and instrument to keep the best score for each document
    val scoreByDocument = this.groupByInstrument(filteredScore, instrumentCodeList)

    //vectorise result
    val feature = this.buildFeaturesFromMaxScore(scoreByDocument)

    //build model to classify positive and negative instruments
                val (model, accuracy) = this.buildRfModel(feature, 150, 30, 9876547687L)
//    val (model, accuracy) = this.buildLrModel(feature, 300, 0.04, 0.44, false, 9876547687L)
    //    val (model, accuracy) = this.buildSVMModel(feature, false, false, 9876547687L)

    Logger.info("Instrument analyser", "Model", s"Accuracy ${accuracy * 100}%")
    DbManager.saveMatchInfo("instrument_analyser_accuracy", accuracy.toString)

    //    predictions
    //      .groupBy($"label", $"prediction")
    //      .count()
    //      .withColumn("accuracy", $"count" / testData.count())
    //      .show()


    //apply lr model to feature
    val prediction = model.transform(feature).cache()

    //format publication result
    val publicationInstrument = prediction
      .select(
        $"entity_id",
        $"row_id",
        $"document_version_id",
        $"instrument_code" as "instrument_name",
        $"duplicated_code",
        $"instrument_sentence",
        $"instrument_confidence",
        $"max_score",
        $"prediction"
      )

    //Merge publication and proposal instrument
    val result = (publicationInstrument union proposalInstrument)
      .join(documentTypeDataFrame, Seq("document_version_id"))

    //save instrument
    if (saveEntities) {
      this.saveResult(result)
    }

    //return
    (EntitiesIdDfType, result
      .filter(($"prediction" === 1 && $"instrument_confidence" =!= -100) || $"instrument_confidence" === 100)
      .filter($"duplicated_code" === false)
      .select($"document_version_id", $"entity_id").distinct())
  }

  def buildFeaturesFromMaxScore(inputData: DataFrame): DataFrame = {

    import inputData.sparkSession.implicits._

    var data = inputData
      .withColumn("exploded_sentences", split($"instrument_sentence", "\\W+"))
      .withColumn("small_word_count", countSmallWord($"exploded_sentences"))
    //      .drop("exploded_sentences")

    //vectorise sentences
    val word2Vec = new Word2Vec()
      .setInputCol("exploded_sentences")
      .setOutputCol("vectorised_sentence")
      .setVectorSize(10)
      .setMinCount(5)
      .setMaxSentenceLength(wordDistanceFromInstrument.toInt * 2 + 1)
      .setSeed(97987676L)
      .setMaxIter(30)
      .setStepSize(0.025)

    val model = word2Vec.fit(data)

    data = model.transform(data).drop($"exploded_sentences")

    //vectorise result
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "row_number",
        "entity_id",
        "instrument_number_score",
        "sentence_instrument_number",
        "instrument_document_position",
        "max_instrument_name_score",
        "max_laboratory_name_score",
        "max_laboratory_short_name_score",
        "max_scientific_technique_score",
        "max_laboratory_country_score",
        "max_laboratory_city_score",
        "max_model_score",
        "max_probability",
        "small_word_count",
        "max_per_instrument_model_score",
        "vectorised_sentence"
      ))
      .setOutputCol("features")


    //apply to data
    val feature = assembler.transform(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(feature)

    val indexedFeatures = featureIndexer.transform(feature)

    Logger.info("Instrument analyser", "Feature", s"feature built")

    //return
    indexedFeatures.cache()
  }


  def buildRfModel(feature: DataFrame, numTrees: Int, maxDepth: Int, seed: Long): (RandomForestClassificationModel, Double) = {

    import feature.sparkSession.implicits._

    //format feature and select training data only
    val rfModelData = feature
      .filter($"instrument_confidence" isin (100 :: -100 :: Nil: _ *))
      .withColumn("label", when($"instrument_confidence" === 100, 1).otherwise(0))
      .select($"label", $"features", $"indexedFeatures")

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = rfModelData.randomSplit(Array(trainingDataFrameRatio, testingDataFrameRatio), seed)

    // Train a RandomForest model.
    val rfClassifier = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(numTrees)
      .setMaxDepth(maxDepth)
      .setMaxBins(32)
      .setSeed(seed)


    // Train model.
    val rfModel = rfClassifier.fit(trainingData)

    // Make predictions.
    val predictions = rfModel.transform(testData).cache()

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    (rfModel, accuracy)
  }

  def buildLrModel(feature: DataFrame, maxIter: Int, regParam: Double, elasticNetParam: Double, fitIntercept: Boolean, seed: Long): (LogisticRegressionModel, Double) = {
    import feature.sparkSession.implicits._

    //format feature and select training data only
    val lrModelData = feature
      .filter($"instrument_confidence" isin (100 :: -100 :: Nil: _ *))
      .withColumn("label", when($"instrument_confidence" === 100, 1).otherwise(0))
      .select($"label", $"features")

    //spilt data for training
    val Array(lrModelDataTrainning, lrModelDataTest) = lrModelData.randomSplit(Array(trainingDataFrameRatio, testingDataFrameRatio), seed)

    //create lr
    val lr = new LogisticRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setFitIntercept(fitIntercept)
      .setFeaturesCol("features")
      .setFamily("binomial")


    // Fit the model
    val lrModel = lr.fit(lrModelDataTrainning)

    // tune the model
    val trainingSummary = lrModel.summary

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Set the model threshold to maximize F-Measure
    val fMeasure = binarySummary.fMeasureByThreshold

    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)

    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)

    //    lrModel.setThreshold(bestThreshold)

    //apply model to test data
    val testPrediction = lrModel.transform(lrModelDataTest).cache()

    //evaluate model prediction
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    //measure error
    val accuracy = evaluator.evaluate(testPrediction)

    //return lr model
    (lrModel, accuracy)
  }

  def buildSVMModel(feature: DataFrame, fitIntercept: Boolean, standardisation: Boolean, seed: Long): (LinearSVCModel, Double) = {

    import feature.sparkSession.implicits._

    //format feature and select training data only
    val rfModelData = feature
      .filter($"instrument_confidence" isin (100 :: -100 :: Nil: _ *))
      .withColumn("label", when($"instrument_confidence" === 100, 1).otherwise(0))
      .select($"label", $"features", $"indexedFeatures")


    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = rfModelData.randomSplit(Array(trainingDataFrameRatio, testingDataFrameRatio), seed)

    // Train a model.
    val classifier = new LinearSVC()
      .setMaxIter(150)
      .setRegParam(0.1)
      .setTol(1E-6)
      .setFitIntercept(fitIntercept)
      .setStandardization(standardisation)


    // Train model.
    val model = classifier.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData).cache()


    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    (model, accuracy)
  }


  def prepareInstrumentName(instrumentNameDataFrame: DataFrame, instrumentAliasDataFrame: DataFrame): DataFrame = {

    val sparkSession = instrumentNameDataFrame.sparkSession

    import sparkSession.implicits._

    //remove black listed
    var result = instrumentNameDataFrame.filter($"instrument_confidence" =!= -100).select(
      $"instrument_code",
      $"instrument_id",
      $"laboratory_id",
      $"instrument_name",
      $"instrument_confidence",
      $"laboratory_name",
      $"laboratory_short_name",
      $"laboratory_address",
      $"laboratory_city",
      $"laboratory_country",
      $"scientific_technique"
    )

    //alias
    val alias = result
      .join(instrumentAliasDataFrame, Seq("instrument_id"))
      .select(
        $"alias" as "instrument_code",
        $"instrument_id",
        $"laboratory_id",
        $"instrument_name",
        $"instrument_confidence",
        $"laboratory_name",
        $"laboratory_short_name",
        $"laboratory_address",
        $"laboratory_city",
        $"laboratory_country",
        $"scientific_technique"
      )

    //Merge alias with original name
    result = result union alias

    //mark instrument with duplicated code
    result = result.groupBy($"instrument_code").agg(
      first($"instrument_id") as "instrument_id",
      first($"laboratory_id") as "laboratory_id",
      first($"instrument_name") as "instrument_name",
      first($"instrument_confidence") as "instrument_confidence",
      first($"laboratory_name") as "laboratory_name",
      first($"laboratory_short_name") as "laboratory_short_name",
      first($"laboratory_address") as "laboratory_address",
      first($"laboratory_city") as "laboratory_city",
      first($"laboratory_country") as "laboratory_country",
      first($"scientific_technique") as "scientific_technique",
      count("*") as "count")
      .withColumn("duplicated_code", when($"count" === 1, lit(false)).otherwise(lit(true)))
      .drop("count")

    broadcast(result)
  }


  def prepareDocumentText(textDf: DataFrame, documentTypeDf: DataFrame): DataFrame = {
    import textDf.sparkSession.implicits._

    //extract words
    val words = textDf.join(documentTypeDf, Seq("document_version_id"))
      .filter($"document_type".startsWith("PROPOSAL") === false)
      .groupBy($"document_version_id", $"document_type")
      .agg(concat_ws(" ", collect_list($"text")) as "text")
      .select($"document_version_id", $"document_type", posexplode(split(lower($"text"), "\\W+")))
      .withColumnRenamed("pos", "word_position")
      .withColumnRenamed("col", "word")
      .filter(length($"word") >= minimumTextWordSize)
      .drop("document_type")

    //get document size
    val wordCountPerDocument = words.groupBy($"document_version_id").agg(max($"word_position") as "document_size")

    //join with word
    val wordWithDocumentSize = words.join(wordCountPerDocument, Seq("document_version_id"))
      .withColumn("document_word_position", $"word_position" / $"document_size")

    wordWithDocumentSize.cache()
  }


  def findInstrumentFromText(words: DataFrame, instrumentNameDf: DataFrame): DataFrame = {

    import words.sparkSession.implicits._

    val explodedInstrumentName = instrumentNameDf.select(
      $"instrument_id" as "entity_id",
      $"instrument_confidence" as "text_instrument_confidence",
      $"instrument_code",
      split($"instrument_name", "\\W+") as "instrument_name_words",
      split($"laboratory_name", "\\W+") as "laboratory_name_words",
      split($"laboratory_short_name", "\\W+") as "laboratory_short_name_words",
      split($"laboratory_city", "\\W+") as "laboratory_city_words",
      split($"laboratory_country", "\\W+") as "laboratory_country_words",
      split(concat_ws(" ", $"scientific_technique"), "\\W+") as "scientific_technique_words",
      split($"instrument_code", "\\W+").getItem(0) as "instrument_code_w1",
      split($"instrument_code", "\\W+").getItem(1) as "instrument_code_w2",
      split($"instrument_code", "\\W+").getItem(2) as "instrument_code_w3",
      split($"instrument_code", "\\W+").getItem(3) as "instrument_code_w4",
      split($"instrument_code", "\\W+").getItem(4) as "instrument_code_w5",
      $"duplicated_code"
    )

    //extract instrument
    val publicationInstrument = words.join(explodedInstrumentName, lower($"word") === lower($"instrument_code_w1")).drop("word")
      .withColumn("row_id", ($"document_version_id" * 10000000L + $"entity_id").cast(StringType))
      .withColumnRenamed("word_position", "instrument_position")
      .withColumnRenamed("document_version_id", "instrument_dv_id")
      .withColumnRenamed("document_word_position", "instrument_document_position")

    //Merge instrument with sentence
    val publicationInstrumentWithSentence = publicationInstrument.join(words, $"instrument_dv_id" === $"document_version_id" && $"word_position" <= $"instrument_position" + wordDistanceFromInstrument && $"word_position" >= $"instrument_position" - wordDistanceFromInstrument)
      .groupBy(
        $"document_version_id",
        $"row_id",
        $"instrument_position",
        $"instrument_document_position",
        $"instrument_code",
        $"duplicated_code",
        $"instrument_name_words",
        $"laboratory_name_words",
        $"laboratory_short_name_words",
        $"laboratory_city_words",
        $"scientific_technique_words",
        $"laboratory_country_words",
        $"instrument_code_w1",
        $"instrument_code_w2",
        $"instrument_code_w3",
        $"instrument_code_w4",
        $"instrument_code_w5",
        $"text_instrument_confidence",
        $"entity_id")
      .agg(
        collect_list("word") as "instrument_sentence"
      )

    publicationInstrumentWithSentence.cache()
  }

  def markTrainingData(analysedInstrument: DataFrame, trainingData: DataFrame): DataFrame = {

    import analysedInstrument.sparkSession.implicits._

    //format data
    val formatedTrainingData = trainingData.select($"row_id", $"decision" * 100 as "decision")

    //join with instrument data
    val data = analysedInstrument.join(formatedTrainingData, Seq("row_id"), "left")
      .withColumn("instrument_confidence", when($"decision".isNotNull, $"decision").otherwise($"text_instrument_confidence"))
      .drop("decision", "text_instrument_confidence")

    data.cache()
  }


  def filterInstrumentWithTooManyOccurencies(instrumentDataFrame: DataFrame, documentCount: Long): DataFrame = {
    import instrumentDataFrame.sparkSession.implicits._

    val instrumentCount = instrumentDataFrame
      .filter($"instrument_confidence" === 0)
      .groupBy($"instrument_code")
      .agg(countDistinct($"document_version_id") as "count")
      .cache()

    val validInstrumentCode = instrumentCount
      .filter($"count" / documentCount < maximumInstrumentOccurencies)
      .select($"instrument_code")

    val invalidInstrumentCode = instrumentCount.filter($"count" / documentCount > maximumInstrumentOccurencies)
      .select($"instrument_code").collect()

    Logger.info("Instrument analyser", "Instrument", s"Invalid instrument list (to many occurencies): ${invalidInstrumentCode.mkString(" ")}")

    val filteredResult = instrumentDataFrame.join(validInstrumentCode, Seq("instrument_code"))

    filteredResult.cache()
  }

  def buildWordModel(instrument: DataFrame, instrumentNameDf: DataFrame): DataFrame = {

    import instrument.sparkSession.implicits._

    val confidentInstrument = instrument.filter($"instrument_confidence" === 100)

    val modelDataCount = confidentInstrument.count()

    val allowedPosTag = "FW" :: "MD" :: "NN" :: "NNS" :: "NNP" :: "NNPS" :: "RP" :: "PDT" :: "UH" :: "VB" :: "VBD" :: "VBG" :: "VBN" :: "VBP" :: "VBZ" :: Nil
    val allowedNerTag = "O" :: Nil

    //create word list
    var model = confidentInstrument
      .select(explode($"instrument_sentence") as "word", $"document_version_id")
      .distinct()
      .drop($"document_version_id")
      .filter(length($"word") >= minimumModelWordSize)
      .filter($"word".isin(stopWords: _*) === false)

    //count word, compute probability and filter with nlp
    model = model
      .groupBy($"word").count()
      .orderBy($"count".desc)
      .withColumn("probabilty", $"count" / modelDataCount)
      .select($"word", $"count", $"probabilty", pos($"word").getItem(0) as "pos_tag", ner($"word").getItem(0) as "ner_tag")
      .filter($"pos_tag".isin(allowedPosTag: _ *))
      .filter($"ner_tag".isin(allowedNerTag: _ *))
      .filter($"probabilty" > wordModelMinProbality)

    //remove instrument name from model
    model = model.join(instrumentNameDf, $"instrument_code" === $"word", "leftanti")
      .orderBy($"probabilty".desc)
      .cache()

    Logger.info("Instrument analyser", "Instrument model", s"Close word probability model successfully built : ${model.select($"word").collect().mkString(" ")}")

    model
  }

  def buildWordModelPerInstrument(instrument: DataFrame, instrumentNameDf: DataFrame): DataFrame = {

    import instrument.sparkSession.implicits._

    val confidentInstrument = instrument.filter($"instrument_confidence" === 100)

    val modelDataCount = confidentInstrument.count()

    val allowedPosTag = "FW" :: "MD" :: "NN" :: "NNS" :: "NNP" :: "NNPS" :: "RP" :: "PDT" :: "UH" :: "VB" :: "VBD" :: "VBG" :: "VBN" :: "VBP" :: "VBZ" :: Nil
    val allowedNerTag = "O" :: Nil

    //create word list
    var model = confidentInstrument
      .select(explode($"instrument_sentence") as "word", $"document_version_id", $"instrument_code")
      .distinct()
      .drop($"document_version_id")
      .filter(length($"word") >= minimumModelWordSize)
      .filter($"word".isin(stopWords: _*) === false)

    //count word, compute probability and filter with nlp
    model = model
      .groupBy($"word", $"instrument_code").count()
      .orderBy($"count".desc)
      .select($"word", $"instrument_code", $"count", pos($"word").getItem(0) as "pos_tag", ner($"word").getItem(0) as "ner_tag")
      .filter($"pos_tag".isin(allowedPosTag: _ *))
      .filter($"ner_tag".isin(allowedNerTag: _ *))
      .filter($"count" >= perInstrumentWordModelMinCount)
      .withColumn("rank", rank().over(Window.partitionBy($"instrument_code").orderBy($"count".desc)))
      .filter($"rank" < perInstrumentWordModelMaxRank)
      .withColumn("instrument_close_word", lit(true))

    //remove instrument name from model
    model = model.join(instrumentNameDf.withColumnRenamed("instrument_code", "instrument_code2"), $"instrument_code2" === $"word", "leftanti")
      .orderBy($"instrument_code", $"count".desc)
      .cache()

    Logger.info("Instrument analyser", "Instrument model", s"Close word per instrument probability model successfully built : ")

    model.select($"instrument_code", $"word")
      .groupBy($"instrument_code")
      .agg(collect_list($"word") as "words")
      .collect()
      .foreach(row => Logger.info("Instrument analyser", "Instrument model", s"${row.getAs[String]("instrument_code")} : ${row.getAs[mutable.WrappedArray[String]]("words").mkString(" ")}"))

    model.drop("rank", "count").cache()
  }

  def scoreInstrumentCandidate(wordModel: DataFrame, perInstrumentWordModel: DataFrame, instrumentCandidate: DataFrame) = {

    import wordModel.sparkSession.implicits._

    //multi word instrument code
    var multiWordFiltered = instrumentCandidate
      .filter(customArrayContains($"instrument_sentence", $"instrument_code_w1", $"instrument_code_w2", $"instrument_code_w3", $"instrument_code_w4", $"instrument_code_w5").or($"instrument_confidence" === 100))
      .drop("document_type",
        "instrument_code_w1",
        "instrument_code_w2",
        "instrument_code_w3",
        "instrument_code_w4",
        "instrument_code_w5")

    //intersection with other instrument data
    val interSectionData = multiWordFiltered
      .withColumn("instrument_name_intersec", interSection($"instrument_sentence", $"instrument_name_words"))
      .withColumn("laboratory_name_intersec", interSection($"instrument_sentence", $"laboratory_name_words"))
      .withColumn("laboratory_short_name_intersec", interSection($"instrument_sentence", $"laboratory_short_name_words"))
      .withColumn("scientific_technique_intersec", interSection($"instrument_sentence", $"scientific_technique_words"))
      .withColumn("laboratory_country_intersec", interSection($"instrument_sentence", $"laboratory_country_words"))
      .withColumn("laboratory_city_intersec", interSection($"instrument_sentence", $"laboratory_city_words"))
      .drop("instrument_name_words", "laboratory_name_words", "scientific_technique_words", "laboratory_country_words", "laboratory_city_words", "laboratory_short_name_words")

    //explode instrument sentence
    val explodedInterSectionData = interSectionData.select(
      $"document_version_id",
      $"row_id",
      $"instrument_code",
      $"duplicated_code",
      $"instrument_position",
      $"instrument_document_position",
      $"entity_id",
      explode($"instrument_sentence") as "word",
      $"instrument_sentence",
      $"instrument_confidence",
      $"instrument_name_intersec",
      $"laboratory_short_name_intersec",
      $"laboratory_name_intersec",
      $"scientific_technique_intersec",
      $"laboratory_country_intersec",
      $"laboratory_city_intersec"
    )

    //join instrument sentence with model
    val explodedWithModelData = explodedInterSectionData
      .join(wordModel.drop("pos_tag", "ner_tag", "count"), Seq("word"), "left")
      .join(perInstrumentWordModel.select($"word", $"instrument_close_word", $"instrument_code"), Seq("word", "instrument_code"), "left")
      .na.fill(0)

    //regroup by instrument
    val scoreToCompute = explodedWithModelData.groupBy(
      $"document_version_id",
      $"row_id",
      $"instrument_code",
      $"duplicated_code",
      $"instrument_position",
      $"instrument_document_position",
      $"instrument_confidence",
      $"entity_id",
      $"instrument_sentence",
      size($"instrument_name_intersec") as "instrument_name_score",
      size($"laboratory_name_intersec") as "laboratory_name_score",
      size($"laboratory_short_name_intersec") as "laboratory_short_name_score",
      size($"scientific_technique_intersec") as "scientific_technique_score",
      size($"laboratory_country_intersec") as "laboratory_country_score",
      size($"laboratory_city_intersec") as "laboratory_city_score"
    ).agg(
      count($"probabilty") as "model_score",
      count($"instrument_close_word") as "per_instrument_model_score",
      sum($"probabilty") as "total_probability",
      collect_list($"probabilty") as "probabilties"
    )

    val scorePerInstrument = scoreToCompute
      .withColumn("score", customSum($"instrument_name_score", $"laboratory_name_score", $"laboratory_short_name_score", $"scientific_technique_score", $"laboratory_country_score", $"laboratory_city_score", $"model_score"))

    scorePerInstrument.cache()
  }

  def groupByInstrument(filteredScore: DataFrame, instrumentCodeArray: Array[String]): DataFrame = {
    import filteredScore.sparkSession.implicits._

    val scoreByDocument = filteredScore
      .orderBy($"document_version_id", $"entity_id", $"score".desc)
      .groupBy($"document_version_id", $"entity_id")
      .agg(
        first($"row_id") as "row_id",
        count("*") as "row_number",
        max($"instrument_confidence") as "instrument_confidence",
        collect_set($"instrument_code") as "instrument_code",
        collect_list($"instrument_position") as "instrument_position",
        first($"instrument_document_position") as "instrument_document_position",
        first($"duplicated_code") as "duplicated_code",
        first(concat_ws(" ", $"instrument_sentence")) as "instrument_sentence",
        collect_list(concat_ws(" ", $"instrument_sentence")) as "instrument_sentences",
        collect_list($"instrument_name_score") as "instrument_name_score",
        collect_list($"laboratory_name_score") as "laboratory_name_score",
        collect_list($"laboratory_short_name_score") as "laboratory_short_name_score",
        collect_list($"scientific_technique_score") as "scientific_technique_score",
        collect_list($"laboratory_country_score") as "laboratory_country_score",
        collect_list($"laboratory_city_score") as "laboratory_city_score",
        collect_list($"model_score") as "model_score",
        collect_list($"total_probability") as "total_probability",
        collect_list($"score") as "score",
        countDistinct($"instrument_code") as "instrument_number_score",
        max($"score") as "max_score",
        max($"instrument_name_score") as "max_instrument_name_score",
        max($"laboratory_name_score") as "max_laboratory_name_score",
        max($"laboratory_short_name_score") as "max_laboratory_short_name_score",
        max($"scientific_technique_score") as "max_scientific_technique_score",
        max($"laboratory_country_score") as "max_laboratory_country_score",
        max($"laboratory_city_score") as "max_laboratory_city_score",
        max($"model_score") as "max_model_score",
        max($"per_instrument_model_score") as "max_per_instrument_model_score",
        max($"total_probability") as "max_probability"
      )

    //count instrumentNumber in sentence
    val result = scoreByDocument
      .withColumn("instrument_codes", lit(instrumentCodeArray))
      .withColumn("sentence_instrument_number", size(interSection($"instrument_code", $"instrument_codes")))
      .drop("instrument_codes")

    result.cache()
  }


  def saveResult(dataFrame: DataFrame) = {
    Logger.info("Instrument analyser", "Instrument model", s"Saving instrument score")

    import dataFrame.sparkSession.implicits._

    dataFrame
      .withColumn("instrument_sentence", customReplace($"instrument_name", $"instrument_sentence"))
      .write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, tableName, dbProperties)
  }


  def customReplace = udf((instruments: mutable.WrappedArray[String], sentence: String) => {
    var senetenceWithHtml = sentence

    instruments.foreach(instrument => senetenceWithHtml = senetenceWithHtml.replace(instrument, "<b>" + instrument + "</b>"))

    senetenceWithHtml
  })


  def customArrayContains = udf((array: mutable.WrappedArray[Any], elem1: Any, elem2: Any, elem3: Any, elem4: Any, elem5: Any) => {
    array.containsSlice(List(elem1, elem2, elem3, elem4, elem5).filterNot(_ == null))
  })

  def interSection = udf((a: mutable.WrappedArray[String], b: mutable.WrappedArray[String]) => a intersect b)

  def countNotZero = udf((elem1: Long, elem2: Long, elem3: Long, elem4: Long, elem5: Long, elem6: Long, elem7: Long) => {
    List(elem1, elem2, elem3, elem4, elem5, elem6, elem7).count(_ > 0)
  })

  def customSum = udf((elem1: Long, elem2: Long, elem3: Long, elem4: Long, elem5: Long, elem6: Long, elem7: Long) => {
    List(elem1, elem2, elem3, elem4, elem5, elem6, elem7).sum
  })

  def countSmallWord = udf((array: mutable.WrappedArray[String]) => array.count(_.length <= 2)
  )

  override def name: String = "Instrument analyser"

  override def maxInputNumber: Int = 1

  override def validInputType: List[DataFrameType] = TextDfType :: Nil

}
