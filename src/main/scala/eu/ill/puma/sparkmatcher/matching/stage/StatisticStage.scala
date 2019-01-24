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

import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, MatchCandidateDfType, PipelineConfig, StatisticStageName}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class StatisticStage(override val input: List[String],
                     override val output: String,
                     val tableName: String,
                     val documentType: DataSource,
                     val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                     val dbProperties: Properties = ProgramConfig.dbProperties) extends Stage(input, output) {
  override def run(matchBuilder: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {


    val documentTypeDF = documentType.loadData._2


    //normalisation
    input.foreach(row => {

      //compute stats
      val stats = this.computeStats(row._2).join(documentTypeDF, Seq("document_version_id"))

      //save them
      stats.write.mode(SaveMode.Append).jdbc(matchingDatabaseUrl, tableName, dbProperties)

    })

    None
  }

  override def name = StatisticStageName

  override def produceData(matchBuilder: PipelineConfig): Boolean = false

  override def acceptAnyInput = false

  override def validInputType(config : PipelineConfig): List[DataFrameType] = MatchCandidateDfType :: Nil

  def computeStats(dataToCompute: DataFrame): DataFrame = {
    import dataToCompute.sparkSession.implicits._

    val filteredDataToCompute = dataToCompute.filter($"score" =!= 0)

    val doc1Df = filteredDataToCompute.groupBy($"document_version1_id" as "document_version_id", $"score_type").agg(collect_list("document_version2_id") as "matched_document_version_ids", collect_list("score") as "scores", collect_list("id") as "match_ids")
    val doc2Df = filteredDataToCompute.groupBy($"document_version2_id" as "document_version_id", $"score_type").agg(collect_list("document_version1_id") as "matched_document_version_ids", collect_list("score") as "scores", collect_list("id") as "match_ids")

    val statDf = doc1Df.union(doc2Df).groupBy($"document_version_id", $"score_type").agg(collect_list("matched_document_version_ids") as "matched_document_version_ids", collect_list("scores") as "scores", collect_list("match_ids") as "match_ids")

    val result = statDf.map(statFunction, encoder)

    result
  }

  def statFunction = new MapFunction[Row, Row]() {
    override def call(row: Row): Row = {
      val doc1Id = row.getAs[Long]("document_version_id")
      val scoreType = row.getString(1)
      val doc2Ids = row.getSeq[mutable.WrappedArray[Long]](2).flatten
      val scores = row.getSeq[mutable.WrappedArray[Double]](3).flatten
      val matchIds = row.getSeq[mutable.WrappedArray[Long]](4).flatten

      val minScore: Double = scores.min
      val maxScore: Double = scores.max
      val matchCount = scores.size

      // rank section
      var docScore: ListBuffer[(Long, Double, Long)] = ListBuffer.empty

      for (i <- scores.indices) {
        docScore.append((doc2Ids(i), scores(i), matchIds(i)))
      }


      docScore = docScore.sortWith(_._2 > _._2)


      val sortedDoc2Id, sortedMatchIds, sortedDoc2Rank: ListBuffer[Long] = ListBuffer.empty
      val sortedDoc2Score: ListBuffer[Double] = ListBuffer.empty

      var rankIndice = 1
      var previousScore = 0.0
      for (i <- docScore.indices) {
        sortedDoc2Id.append(docScore(i)._1)
        sortedDoc2Score.append(docScore(i)._2)
        sortedMatchIds.append(docScore(i)._3)

        if (docScore(i)._2 != previousScore) {
          rankIndice = i + 1
        }
        sortedDoc2Rank.append(rankIndice)

        previousScore = docScore(i)._2
      }


      //stat section
      val stats = new DescriptiveStatistics
      scores.foreach(stats.addValue(_))

      val score10: Double = minScore + (maxScore - minScore) * 0.1
      val score20: Double = minScore + (maxScore - minScore) * 0.2
      val score50: Double = minScore + (maxScore - minScore) * 0.5
      val score80: Double = minScore + (maxScore - minScore) * 0.8
      val score90: Double = minScore + (maxScore - minScore) * 0.9

      val interval10 = scores.count(_ >= score10)
      val interval20 = scores.count(_ >= score20)
      val interval50 = scores.count(_ >= score50)
      val interval80 = scores.count(_ >= score80)
      val interval90 = scores.count(_ >= score90)

      val interval10Percent: Double = 100.0 * interval10.toDouble / matchCount.toDouble
      val interval20Percent: Double = 100.0 * interval20.toDouble / matchCount.toDouble
      val interval50Percent: Double = 100.0 * interval50.toDouble / matchCount.toDouble
      val interval80Percent: Double = 100.0 * interval80.toDouble / matchCount.toDouble
      val interval90Percent: Double = 100.0 * interval90.toDouble / matchCount.toDouble

      Row.fromSeq(List(
        doc1Id,
        scoreType,
        sortedDoc2Id,
        sortedMatchIds,
        sortedDoc2Score,
        sortedDoc2Rank,
        minScore,
        maxScore,
        matchCount,
        stats.getMean,
        stats.getStandardDeviation,
        stats.getPercentile(50),
        interval10,
        interval20,
        interval50,
        interval80,
        interval90,
        score10,
        score20,
        score50,
        score80,
        score90,
        interval10Percent,
        interval20Percent,
        interval50Percent,
        interval80Percent,
        interval90Percent
      ))
    }
  }

  val encoder = RowEncoder.apply(StructType(
    Array(
      StructField("document_version_id", LongType, nullable = false),
      StructField("score_type", StringType, nullable = false),
      StructField("matched_document_version_ids", ArrayType(LongType), nullable = false),
      StructField("match_ids", ArrayType(LongType), nullable = false),
      StructField("scores", ArrayType(DoubleType), nullable = false),
      StructField("rank", ArrayType(LongType), nullable = false),
      StructField("min_score", DoubleType, nullable = false),
      StructField("max_score", DoubleType, nullable = false),
      StructField("match_count", IntegerType, nullable = false),
      StructField("average_score", DoubleType, nullable = false),
      StructField("std", DoubleType, nullable = false),
      StructField("median", DoubleType, nullable = false),
      StructField("percentile10_count", IntegerType, nullable = false),
      StructField("percentile20_count", IntegerType, nullable = false),
      StructField("percentile50_count", IntegerType, nullable = false),
      StructField("percentile80_count", IntegerType, nullable = false),
      StructField("percentile90_count", IntegerType, nullable = false),
      StructField("percentile10", DoubleType, nullable = false),
      StructField("percentile20", DoubleType, nullable = false),
      StructField("percentile50", DoubleType, nullable = false),
      StructField("percentile80", DoubleType, nullable = false),
      StructField("percentile90", DoubleType, nullable = false),
      StructField("percentile10_percent", DoubleType, nullable = false),
      StructField("percentile20_percent", DoubleType, nullable = false),
      StructField("percentile50_percent", DoubleType, nullable = false),
      StructField("percentile80_percent", DoubleType, nullable = false),
      StructField("percentile90_percent", DoubleType, nullable = false)
    )
  ))

  override def isOptional(config: PipelineConfig): Boolean = false

  override def maxInputNumber(config: PipelineConfig): Int = Integer.MAX_VALUE
}
