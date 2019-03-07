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
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

class ScoreListStage(override val input: List[String],
                     override val output: String,
                     val trainingMatchIds: DataSource,
                     val matchingDatabaseUrl: String = ProgramConfig.matchingDatabaseUrl,
                     val dbProperties: Properties = ProgramConfig.dbProperties,
                     val tableName: String = "score_list"
                    ) extends Stage(input, output) {

  override def run(config: PipelineConfig, input: List[(DataFrameType, DataFrame)]): Option[(DataFrameType, DataFrame)] = {

    this.groupMatchCandidate(input.head._2)

    //      Logger.info(this.name.stringValue, config.name, s"Count dataFrame of type(${input.last._1.stringValue}) : ${input.last._2.count()}")

    None
  }


  def groupMatchCandidate(matchCandidateDataFrame: DataFrame) = {
    import matchCandidateDataFrame.sparkSession.implicits._

    //format accepted pairs
    val accepted_pair = trainingMatchIds.loadData._2.select(
      when($"publication_id" < $"proposal_id", $"publication_id").otherwise($"proposal_id") as "document_version1_id",
      when($"publication_id" > $"proposal_id", $"publication_id").otherwise($"proposal_id") as "document_version2_id",
      $"accepted"
    )

    val columnNames = matchCandidateDataFrame.select($"score_type".as[String]).distinct().collectAsList()


    val groupedMatchCandidate = matchCandidateDataFrame
      .groupBy($"pair_id", $"document_version1_id", $"document_version2_id", $"document1_type", $"document2_type")
      .agg(
        count($"*") as "score_type_count",
        collect_list($"score_type") as "scores_type",
        collect_list($"score") as "scores",
        collect_list($"item_count") as "items_count"
      )
      .as[GroupedMatchCandidate]


    val scoreList = groupedMatchCandidate.map(matchCandidate => {
      val scoreMap = (matchCandidate.scores_type zip matchCandidate.scores).toMap

      (
        matchCandidate.pair_id,
        matchCandidate.document_version1_id,
        matchCandidate.document_version2_id,
        matchCandidate.document1_type,
        matchCandidate.document2_type,
        matchCandidate.score_type_count,
        scoreMap.get(DoiType.stringValue),
        scoreMap.get(ReferencedDoiType.stringValue),
        scoreMap.get(ProposalCodeType.stringValue),
        scoreMap.get(ReferencedProposalCodeType.stringValue),
        scoreMap.get(PictureType.stringValue),
        scoreMap.get(InstrumentType.stringValue),
        scoreMap.get(AbstractType.stringValue),
        scoreMap.get(TextType.stringValue),
        scoreMap.get(CosineTitleType.stringValue),
        scoreMap.get(PersonType.stringValue),
        scoreMap.get(FormulaType.stringValue)
      )
    })
      .toDF(
        "pair_id",
        "document_version1_id",
        "document_version2_id",
        "document1_type",
        "document2_type",
        "score_type_count",
        DoiType.stringValue + "_score",
        ReferencedDoiType.stringValue + "_score",
        ProposalCodeType.stringValue + "_score",
        ReferencedProposalCodeType.stringValue + "_score",
        PictureType.stringValue + "_score",
        InstrumentType.stringValue + "_score",
        AbstractType.stringValue + "_score",
        TextType.stringValue + "_score",
        CosineTitleType.stringValue + "_score",
        PersonType.stringValue + "_score",
        FormulaType.stringValue + "_score"
      )
      .join(accepted_pair, Seq("document_version1_id", "document_version2_id"), "left")
      .na.fill(false, Seq("accepted"))

    scoreList.write.mode(SaveMode.Overwrite).jdbc(matchingDatabaseUrl, tableName, dbProperties)

  }

  override def name = ScoreListingStageName

  override def produceData(config: PipelineConfig): Boolean = false

  override def acceptAnyInput = false

  override def validInputType(config: PipelineConfig): List[DataFrameType] = MatchCandidateDfType :: Nil

  override def maxInputNumber(config: PipelineConfig): Int = 1

  override def isOptional(config: PipelineConfig): Boolean = false
}

case class GroupedMatchCandidate(pair_id: Long,
                                 document_version1_id: Long,
                                 document_version2_id: Long,
                                 document1_type: String,
                                 document2_type: String,
                                 score_type_count: Long,
                                 scores_type: List[String],
                                 scores: List[Double],
                                 items_count: List[Long])

