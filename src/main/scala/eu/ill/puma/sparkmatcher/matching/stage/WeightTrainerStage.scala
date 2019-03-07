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
package eu.ill.puma.sparkmatcher.matching.stage

import java.util
import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

class WeightTrainerStage(override val input: List[String],
                         override val output: String,
                         val windowsSize: Int,
                         val areaNumberToEvaluate: Int,
                         val trainingMatchIds: DataSource,
                         val tableName: String = "score_weight",
                         val positiveErrorValue: Double = 1,
                         val negativeErrorValue: Double = 1,
                         val thresholdValue: Int = 3,
                         val doiWeight: Double = ProgramConfig.optimizerDoiWeight,
                         val proposalCodeWeight: Double = ProgramConfig.optimizerProposalCodeWeight,
                         val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                         val dbProperties: Properties = ProgramConfig.dbProperties
                        ) extends Stage(input, output) {

  private val typeToOptimize: ListBuffer[EntityType] = ListBuffer.empty[EntityType]
  private val publicationTrainingIds = trainingMatchIds.loadData._2.select("publication_id").distinct().collect().map(_.getLong(0))
  private val pairTrainingIds = trainingMatchIds.loadData._2.select("publication_id", "proposal_id").distinct().collect().map(x => x.getLong(0) * 1000000000L + x.getLong(1))
  private val acceptedPairTrainingIds = trainingMatchIds.loadData._2.filter(col("accepted") === true).select("publication_id", "proposal_id").distinct().collect().map(x => x.getLong(0) * 1000000000L + x.getLong(1))

  /**
    * Run the stage
    *
    * @param matchBuilder
    * @param input
    * @return
    */
  override def run(matchBuilder: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {

    /**
      * Init
      */
    Logger.info("Optimizer", "Training Stage", s"Evaluate top $areaNumberToEvaluate area")

    val matchCandidate = this.prepareMatchCandidate(input.head._2)
    val sparkSession = matchCandidate.sparkSession
    val initialStep = BigDecimal(0.5) / BigDecimal(windowsSize)
    val resultList = new util.ArrayList[Row]

    import sparkSession.implicits._

    //prepare match candidate

    /**
      * first iteration
      */
    val bestAreaDF = Cartographer.optimize(matchCandidate, typeToOptimize.map(e => (e.stringValue, BigDecimal(0.0))).toList, initialStep, windowsSize).limit(areaNumberToEvaluate).cache
    val bestArea = bestAreaDF.collect()

    //    show candidate
    Logger.info(this.name.stringValue, matchBuilder.name, s"First iteration complete, selected candidate :")
    bestAreaDF.show

    var paths = mutable.Map[String, Row]()

    /**
      * area loop
      */
    var areacounter = 1
    bestArea.par.foreach(areaToexplore => {

      val areaNb = areacounter
      areacounter += 1

      var error = Double.MaxValue / 10.0
      var previousIterationError = Double.MaxValue
      var previousIterationResult = this.collectTopScore(areaToexplore)
      var iteration = 0
      var step = initialStep
      var topResult: Row = null
      val evaluatedRows = ListBuffer.empty[Row]
      var overlapping = false

      /**
        * Optimisation loop
        */
      while (error < previousIterationError && overlapping == false) {

        //update step
        step = step / BigDecimal(2)

        //launch optimisation
        topResult = Walker.optimize(matchCandidate, previousIterationResult, step, 1).head
        iteration += 1

        //Save path
        evaluatedRows.append(topResult)

        //prepare next iteration
        previousIterationResult = this.collectTopScore(topResult)
        previousIterationError = error
        error = topResult.getAs[Double]("error")

        //log
        Logger.info(this.name.stringValue, matchBuilder.name, s"Area $areaNb, Iteration $iteration complete, step : $step, error $error")

        //area overlapping
        if (paths.contains(buildIdString(topResult))) {
          Logger.warn(this.name.stringValue, matchBuilder.name, s"Area $areaNb, Iteration $iteration, area $areaNb interrupted due to overlapping with other area")

          topResult = paths(buildIdString(topResult))

          overlapping = true
        }
      }

      if (overlapping == false) {
        evaluatedRows.foreach(row => paths += (buildIdString(row) -> topResult))
        //end area
        Logger.info(this.name.stringValue, matchBuilder.name, s"Area $areaNb complete, error : $error")
        resultList.add(topResult)
      }

    })

    Logger.info(this.name.stringValue, matchBuilder.name, s"Best area exploration complete, result :")
    val resultsDf = sparkSession.createDataFrame(resultList, bestAreaDF.schema).orderBy(col("error").asc)

    resultsDf.show(areaNumberToEvaluate)

    /**
      * Save result
      */

    val finalWeight = ListBuffer.empty[(String, Double)]

    finalWeight.appendAll(this.collectTopScore(resultsDf.take(1)(0)).map(e => (e._1, e._2.toDouble)))
    finalWeight.append((DoiType.stringValue, doiWeight))
    finalWeight.append((ProposalCodeType.stringValue, proposalCodeWeight))

    val result = finalWeight.toDF("score_type", "weight")

    result.write.mode(SaveMode.Overwrite).jdbc(matchingDatabaseUrl, tableName, dbProperties)

    //return
    Some(WeightEvaluatorDfType, result)
  }


  /**
    * prepare the match candidate and add the virtual matcher : matcher number
    * add publication and proposal id
    * switch to contextual score
    *
    * @param matchCandidate
    * @return
    */
  def prepareMatchCandidate(matchCandidate: DataFrame): DataFrame = {
    import matchCandidate.sparkSession.implicits._

    //switch to contextual score
    val window = Window.partitionBy($"publication_id", $"score_type")

    val result = matchCandidate.select($"id", $"document_version1_id", $"document_version2_id", $"document1_type", $"document2_type", $"score_type", $"score", $"item_count", $"pair_id")
      .withColumn("proposal_id", when($"document1_type".startsWith("PROPOSAL"), $"document_version1_id").otherwise($"document_version2_id"))
      .withColumn("publication_id", ($"document_version1_id" * $"document_version2_id") / $"proposal_id")
      .drop("document1_type", "document2_type", "pair_id", "item_count", "id")
      .withColumn("max", max($"score").over(window))
      .withColumn("score", $"score" / $"max" * 100)
      .filter($"publication_id".isin(publicationTrainingIds: _*))
      .repartition($"publication_id")

    broadcast(result)
  }

  object Walker {
    def optimize(matchCandidate: DataFrame, previousResult: List[(String, BigDecimal)], step: BigDecimal, windowsSize: Int): DataFrame = {
      val sparkSession = matchCandidate.sparkSession

      import sparkSession.implicits._

      //generate weight combination
      val weightDataFrame = this.generateWeight(sparkSession, previousResult, step, windowsSize)

      //apply all weight
      val totalMatchCandidate = this.computeWeight(matchCandidate, weightDataFrame)

      //compute rank
      val rankedMatchCandidate = this.computeRank(totalMatchCandidate)

      //      sum(when($"rank" <= lit(10) and $"accepted" === true , 1).when($"rank" <= lit(10) and $"accepted" === false , -1).otherwise(0)) as "top10",


      //evaluator
      val bestWeight = rankedMatchCandidate
        .filter(($"publication_id" * 1000000000 + $"proposal_id").isin(pairTrainingIds: _*))
        .withColumn("accepted", when(($"publication_id" * 1000000000 + $"proposal_id").isin(acceptedPairTrainingIds: _*), true).otherwise(false))
        .withColumn("rank", when($"total_score" === 0, 10).otherwise($"rank"))
        .groupBy("weight_id")
        .agg(
          sum($"rank" - 1).cast(DoubleType) as "sum",
          sum(when($"rank" <= lit(10) and $"accepted" === true, 1).otherwise(0)) as "top10",
          sum(when($"rank" <= lit(thresholdValue) and $"accepted" === true, 1).otherwise(0)) as "top" + thresholdValue,
          sum(when($"rank" <= lit(5) and $"accepted" === true, 1).otherwise(0)) as "top5",
          sum(when($"rank" <= lit(3) and $"accepted" === true, 1).otherwise(0)) as "top3",
          sum(when($"rank" <= lit(1) and $"accepted" === true, 1).otherwise(0)) as "top1",
          sum(when($"rank" > lit(thresholdValue) and $"accepted" === true, positiveErrorValue)
            .when($"rank" <= lit(thresholdValue) and $"accepted" === false, negativeErrorValue)
            .otherwise(0).cast(DoubleType)) as "error",
          sum(when($"rank" > lit(thresholdValue) and $"accepted" === true, 1).otherwise(0).cast(DoubleType)) as "positive_error_count",
          sum(when($"rank" <= lit(thresholdValue) and $"accepted" === false, 1).otherwise(0).cast(DoubleType)) as "negative_error_count"
        )
        .join(weightDataFrame, $"weight_id" === $"id")
        .orderBy($"error".asc)
        .drop("id")

      bestWeight
    }

    /**
      * compute the rank
      *
      * @param dataFrame
      * @return
      */
    def computeRank(dataFrame: DataFrame): DataFrame = {

      import dataFrame.sparkSession.implicits._

      val window = Window.partitionBy($"weight_id", $"publication_id").orderBy($"total_score".desc)

      dataFrame.select($"weight_id", $"publication_id", $"proposal_id", $"total_score", rank.over(window).alias("rank"))
    }

    /**
      * apply the possible weight to the match candidate
      *
      * @param typedMatchCandidate the match candidate where the weight are applied
      * @param weightDataFrame     the weight dataframe
      * @return
      */
    def computeWeight(typedMatchCandidate: DataFrame, weightDataFrame: DataFrame): DataFrame = {

      //init
      import typedMatchCandidate.sparkSession.implicits._

      //get type to join
      val typeName = weightDataFrame.drop("id").schema.fieldNames.toList

      //create empty weight df
      var weight = Seq.empty[(Long, Double, String)].toDF("id", "value", "type")

      //fill weight df
      typeName.foreach(name => {
        weight = weightDataFrame.select($"id" as "weight_id", col(name) as "weight", lit(name) as "score_type") union weight
      })

      weight = broadcast(weight)

      //apply weight
      val matchCandidateWithWeight = typedMatchCandidate.join(weight, Seq("score_type"))
        .withColumn("weight_score", $"score" * $"weight")
        .drop("score", "weight", "score_type")
        .repartition($"weight_id", $"publication_id")

      //compute total score
      val totalMatchCandidate = matchCandidateWithWeight.groupBy($"weight_id", $"publication_id", $"proposal_id").agg(
        sum($"weight_score") as "total_score"
      )

      totalMatchCandidate
    }

    /**
      * generate possible weight
      *
      * @param sparkSession   sparksession, required
      * @param previousResult name of the weight
      * @param step           the increment of the weight
      * @return
      */
    def generateWeight(sparkSession: SparkSession, previousResult: List[(String, BigDecimal)], step: BigDecimal, windowsSize: Int): DataFrame = {
      import sparkSession.implicits._

      val typeWeight = ListBuffer.empty[(String, List[BigDecimal])]
      val maxParameterSumValue = BigDecimal(1)

      //possible weight init
      previousResult.foreach(typeName => {
        typeWeight.append((typeName._1, generateRangeList(typeName._2, step, windowsSize)))
      })

      //weight generation init
      var weightDataFrame = typeWeight.head._2.toDF(typeWeight.head._1)
      var columnSumExpression = col(typeWeight.head._1)

      //weight generation
      for (i <- 1 to (typeWeight.size - 1)) {
        //colmun to add
        val currentDataFrame = broadcast(typeWeight(i)._2.toDF(typeWeight(i)._1))

        //update columnSum
        columnSumExpression = columnSumExpression + col(typeWeight(i)._1)

        //add column
        weightDataFrame = weightDataFrame.join(currentDataFrame, columnSumExpression <= maxParameterSumValue)
      }

      //final filter
      weightDataFrame = weightDataFrame.filter(columnSumExpression === maxParameterSumValue)

      //add id and filter values
      weightDataFrame = weightDataFrame
        .distinct()
        .columns.foldLeft(weightDataFrame)((current, c) => current.withColumn(c, col(c).cast(DoubleType)))
        .withColumn("id", monotonically_increasing_id)
        .cache()

      //count parameter number
      var parameterNb = 0
      typeWeight.foreach(parameterSet => {
        parameterNb += parameterSet._2.size
      })

      //return
      weightDataFrame
    }
  }

  object Cartographer {

    def optimize(matchCandidate: DataFrame, previousResult: List[(String, BigDecimal)], step: BigDecimal, windowsSize: Int): DataFrame = {

      val sparkSession = matchCandidate.sparkSession

      import sparkSession.implicits._

      //generate weight combination
      val weight = this.generateWeight(sparkSession, previousResult, step, windowsSize).cache()

      //create empty exploded weight df
      var explodedWeight = Seq.empty[(Long, Double, String)].toDF("weight_id", "value", "type")

      //fill exploded weight df
      previousResult.foreach(name => {
        explodedWeight = weight.select($"weight_id", col(name._1) as "weight", lit(name._1) as "score_type") union explodedWeight
      })

      //share result
      explodedWeight = broadcast(explodedWeight)

      //apply all weight
      val matchCandidateWithWeight = matchCandidate.join(explodedWeight, Seq("score_type"))
        .withColumn("weight_score", $"score" * $"weight")
        .drop("score", "weight", "score_type")

      //compute total score
      val totalMatchCandidate = matchCandidateWithWeight.groupBy($"publication_id", $"proposal_id", $"weight_id").agg(
        sum($"weight_score") as "total_score"
      )

      val window = Window.partitionBy($"publication_id", $"weight_id").orderBy($"total_score".desc)

      //compute rank
      val rankedMatchCandidate = totalMatchCandidate.select($"weight_id", $"publication_id", $"proposal_id", $"total_score", rank.over(window).alias("rank"))

      //evaluator
      var bestWeight = rankedMatchCandidate
        .filter(($"publication_id" * 1000000000 + $"proposal_id").isin(pairTrainingIds: _*))
        .withColumn("accepted", when(($"publication_id" * 1000000000 + $"proposal_id").isin(acceptedPairTrainingIds: _*), true).otherwise(false))
        .withColumn("rank", when($"total_score" === 0, 10).otherwise($"rank"))
        .groupBy("weight_id")
        .agg(
          sum($"rank" - 1).cast(DoubleType) as "sum",
          sum(when($"rank" <= lit(10) and $"accepted" === true, 1).otherwise(0)) as "top10",
          sum(when($"rank" <= lit(thresholdValue) and $"accepted" === true, 1).otherwise(0)) as "top" + thresholdValue,
          sum(when($"rank" <= lit(5) and $"accepted" === true, 1).otherwise(0)) as "top5",
          sum(when($"rank" <= lit(3) and $"accepted" === true, 1).otherwise(0)) as "top3",
          sum(when($"rank" <= lit(1) and $"accepted" === true, 1).otherwise(0)) as "top1",
          sum(when($"rank" > lit(thresholdValue) and $"accepted" === true, positiveErrorValue)
            .when($"rank" <= lit(thresholdValue) and $"accepted" === false, negativeErrorValue)
            .otherwise(0).cast(DoubleType)) as "error",
          sum(when($"rank" > lit(thresholdValue) and $"accepted" === true, 1).otherwise(0).cast(DoubleType)) as "positive_error_count",
          sum(when($"rank" <= lit(thresholdValue) and $"accepted" === false, 1).otherwise(0).cast(DoubleType)) as "negative_error_count"
        )

      bestWeight = bestWeight.join(weight, Seq("weight_id"))
        .orderBy($"error".asc)
        .drop("id")

      //unpersist Weight
      weight.unpersist()

      bestWeight
    }

    /**
      * generate possible weight
      *
      * @param sparkSession   sparkSession, required
      * @param previousResult name of the weight
      * @param step           the increment of the weight
      * @return
      */
    def generateWeight(sparkSession: SparkSession, previousResult: List[(String, BigDecimal)], step: BigDecimal, windowsSize: Int): DataFrame = {
      import sparkSession.implicits._

      val typeWeight = ListBuffer.empty[(String, List[BigDecimal])]
      val maxParameterSumValue = BigDecimal(1)

      //possible weight init
      previousResult.foreach(typeName => {
        typeWeight.append((typeName._1, generateRangeList(typeName._2, step, windowsSize)))
      })

      //weight generation init
      var weightDataFrame = typeWeight.head._2.toDF(typeWeight.head._1)
      var columnSumExpression = col(typeWeight.head._1)

      //weight generation
      for (i <- 1 to (typeWeight.size - 1)) {
        //colmun to add
        val currentDataFrame = broadcast(typeWeight(i)._2.toDF(typeWeight(i)._1))

        //update columnSum
        columnSumExpression = columnSumExpression + col(typeWeight(i)._1)

        //add column
        weightDataFrame = weightDataFrame.join(currentDataFrame, columnSumExpression <= maxParameterSumValue)
      }

      //final filter
      weightDataFrame = weightDataFrame.filter(columnSumExpression === maxParameterSumValue)

      //add id and filter values
      weightDataFrame = weightDataFrame
        .distinct()
        .columns.foldLeft(weightDataFrame)((current, c) => current.withColumn(c, col(c).cast(DoubleType)))
        .withColumn("weight_id", monotonically_increasing_id)

      //return
      weightDataFrame
    }
  }




  /**
    * genereate  a list of successive value
    *
    * @param middle
    * @param step
    * @return
    */
  def generateRangeList(middle: BigDecimal, step: BigDecimal, windowsSize: Int): List[BigDecimal] = {
    //create weight list value
    val valueList = ListBuffer.empty[BigDecimal]

    valueList.append(middle)

    for (i <- 1 to windowsSize) {
      val current = middle + (step * i).setScale(20, RoundingMode.HALF_UP)

      if (current <= 1 && current >= 0) {
        valueList.append(current)
      }
    }

    for (i <- 1 to windowsSize) {
      val current = middle - (step * i).setScale(20, RoundingMode.HALF_UP)

      if (current <= 1 && current >= 0) {
        valueList.append(current)
      }
    }

    valueList.toList.sorted
  }


  /**
    * based on a previous iteration, calculate the next bound for the next iteration
    *
    * @param resultRow
    * @return
    */
  def collectTopScore(resultRow: Row): List[(String, BigDecimal)] = {
    val nextValueList = ListBuffer.empty[(String, BigDecimal)]

    //foreach type
    this.typeToOptimize.foreach(typeName => {
      val result = BigDecimal(resultRow.getAs[Double](typeName.stringValue))

      nextValueList.append((typeName.stringValue, result))
    })

    nextValueList.toList
  }

  def buildIdString(resultRow: Row): String = {
    var idString = ""

    //foreach type
    this.typeToOptimize.foreach(typeName => {
      val result = BigDecimal(resultRow.getAs[Double](typeName.stringValue))
      idString += typeName.stringValue + " " + result + " "
    })

    idString
  }

  def addType(entityType: EntityType) = typeToOptimize.append(entityType)

  /**
    * return the nae of the stage
    *
    * @return
    */
  override def name = WeightEvaluatorStageName

  /**
    * return true if the stage produce some data
    *
    * @param matchBuilder
    * @return
    */
  override def produceData(matchBuilder: PipelineConfig): Boolean = true

  /**
    * return true if the stage accept any input type
    *
    * @return
    */
  override def acceptAnyInput = false

  /**
    * define the valid inputtype accepted by the stage
    *
    * @param config
    * @return
    */
  override def validInputType(config: PipelineConfig): List[DataFrameType] = MatchCandidateDfType :: Nil

  /**
    * return true if the stage can be skip by the pipeline
    *
    * @param config
    * @return
    */
  override def isOptional(config: PipelineConfig): Boolean = false

  /**
    * Set the macimum input number of the stage
    *
    * @param config
    * @return
    */
  override def maxInputNumber(config: PipelineConfig): Int = Integer.MAX_VALUE

}

object WeightTrainerStage {
  val weightEvaluatorOutputSchema = StructType(Array(
    StructField("score_type", StringType, nullable = false),
    StructField("weight", DoubleType, nullable = false)
  ))
}
