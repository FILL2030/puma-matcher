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
package eu.ill.puma.sparkmatcher.matching.app

import java.time.LocalDateTime

import eu.ill.puma.sparkmatcher.matching.analyser._
import eu.ill.puma.sparkmatcher.matching.datasource.DataSourceStorage
import eu.ill.puma.sparkmatcher.matching.filter.{DateFilter, DocumentVersionIdFilter, MultipleTypeMatchFilter, TypeFilter}
import eu.ill.puma.sparkmatcher.matching.matcheditor.HundredMatcherAnalyser
import eu.ill.puma.sparkmatcher.matching.matcher._
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.matching.scorer._
import eu.ill.puma.sparkmatcher.matching.stage._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SaveMode, SparkSession}

object FullMatcherApp {


  def main(args: Array[String]): Unit = {

    //Matcher
    this.runMatcher

    //optimiser
    this.runOptimizer

    //run wordSpec
    this.runWordSpec

    //final db management
    DbManager.saveMatchInfo("end_date", LocalDateTime.now().toString)

    //utils
    if (ProgramConfig.prod) {
      if (Logger.errorOccured) {
        Logger.error("App", "no config", s"Schema swaping canceled, some error occured during matcher execution")
      } else {
        DbManager.swapSchema
      }
    }

    //check results
    RankEvaluatorApp.run

  }


  def runMatcher = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)

    Logger.info("App", "no config", s"running on database : ${ProgramConfig.matchingDatabaseUrl}")
    DbManager.fullReset

    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Matcher")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    //create data source storage
    val dataSourceStorage = new DataSourceStorage(sparkSession)

    import sparkSession.implicits._

    dataSourceStorage.fileDataSource.loadData._2
      .select($"document_version_id", regexp_replace($"text","""[^a-zA-Z0-9;:,.?!\- ]""", " ") as "text", $"file_path")
      .write.mode(SaveMode.Overwrite).jdbc(ProgramConfig.matchingDatabaseUrl, "full_text", ProgramConfig.dbProperties)


    //import spark implicit

    /**
      * first level matcher pipeline
      */

    //create pipepline
    val matchCreatorPipeline = new Pipeline("match creator pipeline")

    val personMatchConfig = new PipelineConfig("person config")
    personMatchConfig.dataSource = dataSourceStorage.personDataSource
    personMatchConfig.matcher = new EntitiesMatcherV2(false)
    personMatchConfig.scorer = new EntitiesScorer
    personMatchConfig.matchEntityType = PersonType

    val laboratoryMatchConfig = new PipelineConfig("laboratory config")
    laboratoryMatchConfig.dataSource = dataSourceStorage.laboratoryDataSource
    laboratoryMatchConfig.matcher = new EntitiesMatcherV2
    laboratoryMatchConfig.scorer = new EntitiesScorer
    laboratoryMatchConfig.matchEntityType = LaboratoryType

    val formulaMatchConfig = new PipelineConfig("formula config")
    formulaMatchConfig.dataSource = dataSourceStorage.fileDataSource
    formulaMatchConfig.analyser = new FormulaAnalyser( dataSourceStorage.documentAddressDataSource)
    formulaMatchConfig.matcher = new EntitiesMatcherV2
    formulaMatchConfig.scorer = new EntitiesScorer
    formulaMatchConfig.matchEntityType = FormulaType

    val doiMatchConfig = new PipelineConfig("doi config")
    doiMatchConfig.dataSource = dataSourceStorage.doiDataSource
    doiMatchConfig.analyser = new DoiAnalyser(dataSourceStorage.fileDataSource)
    doiMatchConfig.matcher = new DualTypeEntitiesMatcher(DoiType, ReferencedDoiType)
    doiMatchConfig.scorer = new EntitiesScorer
    doiMatchConfig.addMatchEditor(new HundredMatcherAnalyser)
    doiMatchConfig.matchEntityType = DoiType

    val proposalCodeMatchConfig = new PipelineConfig("proposal code config")
    proposalCodeMatchConfig.dataSource = dataSourceStorage.proposalCodeDataSource
    proposalCodeMatchConfig.analyser = new ProposalCodeAnalyser(dataSourceStorage.fileDataSource)
    proposalCodeMatchConfig.matcher = new DualTypeEntitiesMatcher(ProposalCodeType, ReferencedProposalCodeType)
    proposalCodeMatchConfig.scorer = new EntitiesScorer
    proposalCodeMatchConfig.addMatchEditor(new HundredMatcherAnalyser)
    proposalCodeMatchConfig.matchEntityType = ProposalCodeType

    val abstractMatchConfig = new PipelineConfig("abstract config")
    abstractMatchConfig.dataSource = dataSourceStorage.textAbstractDataSource
    abstractMatchConfig.matcher = new TextMatcher(4)
    abstractMatchConfig.scorer = new TextScorer
    abstractMatchConfig.matchEntityType = AbstractType

    val textMatchConfig = new PipelineConfig("text config")
    textMatchConfig.dataSource = dataSourceStorage.advFileDataSource
    textMatchConfig.matcher = new TextMatcher
    textMatchConfig.scorer = new TextScorer
    textMatchConfig.matchEntityType = TextType

    val cosineMatchConfig = new PipelineConfig("cosine config")
    cosineMatchConfig.dataSource = dataSourceStorage.titleDataSource
    cosineMatchConfig.matcher = new CosineMatcher(dataSourceStorage.advFileDataSource)
    cosineMatchConfig.scorer = new CosineScorer
    cosineMatchConfig.matchEntityType = CosineTitleType

    val instrumentConfig = new PipelineConfig("instrument config")
    instrumentConfig.dataSource = dataSourceStorage.fileDataSource
    //instrumentConfig.analyser = new InstrumentAnalyser(dataSourceStorage.instrumentDataSource, dataSourceStorage.instrumentNameDataSource, dataSourceStorage.typeDataSource)
    instrumentConfig.analyser = new AdvancedInstrumentAnalyser(dataSourceStorage.instrumentDataSource, dataSourceStorage.instrumentNameDataSource, dataSourceStorage.instrumentAliasDataSource, dataSourceStorage.typeDataSource, dataSourceStorage.trainingDataDataSource, "instrument")
    instrumentConfig.matcher = new EntitiesMatcherV2(false)
    instrumentConfig.scorer = new EntitiesScorer
    instrumentConfig.matchEntityType = InstrumentType

    //create stage
    val matchCreatorInitialisationStage = new InitialisationStage(Nil, "init")

    val matchCreatorAnalyserStage = new AnalyserStage("init" :: Nil, "analyzer_output")

    val matchCreatorMatcherStage = new MatcherStage("analyzer_output" :: Nil, "matcher_output")

    val matchCreatorFilterStage = new FilterStage("matcher_output" :: Nil, "filter_output")
    matchCreatorFilterStage.addFilter(new TypeFilter(dataSourceStorage.typeDataSource))
    matchCreatorFilterStage.addFilter(new DocumentVersionIdFilter(dataSourceStorage.validDocumentVersionIdDataSource))
    matchCreatorFilterStage.addFilter(new DateFilter(dataSourceStorage.dateDateSource, dataSourceStorage.documentVersionSourceDataSource))

    val matchCreatorScoringStage = new ScoringStage("filter_output" :: Nil, "scorer_output")

    val matchCreatorNormalisationStage = new NormalisationStage("scorer_output" :: Nil, "normalisation_output")

    val matchCreatorEditorStage = new MatchEditorStage("normalisation_output" :: Nil, "editor_output")

    // add stage to pipeline
    matchCreatorPipeline.addStage(matchCreatorInitialisationStage)
    matchCreatorPipeline.addStage(matchCreatorAnalyserStage)
    matchCreatorPipeline.addStage(matchCreatorMatcherStage)
    matchCreatorPipeline.addStage(matchCreatorFilterStage)
    matchCreatorPipeline.addStage(matchCreatorScoringStage)
    matchCreatorPipeline.addStage(matchCreatorNormalisationStage)
    matchCreatorPipeline.addStage(matchCreatorEditorStage)

    //add matcher builder to pipeline
    matchCreatorPipeline.addConfig(personMatchConfig)
    matchCreatorPipeline.addConfig(laboratoryMatchConfig)
    matchCreatorPipeline.addConfig(formulaMatchConfig)
    matchCreatorPipeline.addConfig(doiMatchConfig)
    matchCreatorPipeline.addConfig(proposalCodeMatchConfig)
    matchCreatorPipeline.addConfig(textMatchConfig)
    matchCreatorPipeline.addConfig(abstractMatchConfig)
    matchCreatorPipeline.addConfig(cosineMatchConfig)
    matchCreatorPipeline.addConfig(instrumentConfig)

    //foreach matchbuilder and foreach stage run !
    matchCreatorPipeline.run()

    DbManager.saveMatchInfo("matcher_creator_end_date", LocalDateTime.now().toString)


    /**
      * matcher saver/filter pipeline pipeline
      */

    //create pipeline
    val matchSaverPipeline = new Pipeline("match saver pipeline")

    //create config
    val matchSaverConfig = new PipelineConfig("match saver config")
    matchSaverConfig.dataSource = matchCreatorPipeline.asDataSource("editor_output", MatchCandidateWithoutTypeDfType)

    //Create stage
    val matchSaverInitialisationStage = new InitialisationStage(Nil, "init")

    val matchSaverFilterStage = new FilterStage("init" :: Nil, "filter_output")
    matchSaverFilterStage.addFilter(new MultipleTypeMatchFilter(PersonType))

    val matchSaverPersisterStage = new MatchCandidatePersisterStage("filter_output" :: Nil, "", "match_candidate", dataSourceStorage.typeDataSource)

    val matchSaverTrainingDataExtractionStage = new TrainingDataExtractionStage("filter_output" :: Nil, "", dataSourceStorage.trainingIdsDataSource)

    val matchSaverStatisticStage = new StatisticStage("filter_output" :: Nil, "", "match_candidate_stats", dataSourceStorage.typeDataSource)

    //add stage
    matchSaverPipeline.addStage(matchSaverInitialisationStage)
    matchSaverPipeline.addStage(matchSaverFilterStage)
    matchSaverPipeline.addStage(matchSaverPersisterStage)
    matchSaverPipeline.addStage(matchSaverTrainingDataExtractionStage)
    matchSaverPipeline.addStage(matchSaverStatisticStage)

    //add config
    matchSaverPipeline.addConfig(matchSaverConfig)

    //run !
    matchSaverPipeline.run()

    DbManager.saveMatchInfo("matcher_saver_end_date", LocalDateTime.now().toString)

    /**
      * second level matcher pipeline
      */

    //create pipeline
    val picturePipeline = new Pipeline("picture pipeline")

    //create stage
    val picturePipelineInitialisationStage = new InitialisationStage(Nil, "init")

    val picturePipelineAnalyserStage = new AnalyserStage("init" :: Nil, "analyzer_output")

    val picturePipelineMatcherStage = new MatcherStage("analyzer_output" :: Nil, "matcher_output")

    val picturePipelineScoringStage = new ScoringStage("matcher_output" :: Nil, "scorer_output")

    val picturePipelineNormalisationStage = new NormalisationStage("scorer_output" :: Nil, "normalisation_output")

    val picturePipelinePersisterStage = new MatchCandidatePersisterStage("normalisation_output" :: Nil, "", "match_candidate", dataSourceStorage.typeDataSource)

    val picturePipelineTrainingDataExtractionStage = new TrainingDataExtractionStage("normalisation_output" :: Nil, "", dataSourceStorage.trainingIdsDataSource)

    val picturePipelineStatisticStage = new StatisticStage("normalisation_output" :: Nil, "", "match_candidate_stats", dataSourceStorage.typeDataSource)


    //create config
    val pictureMatchConfig = new PipelineConfig("picture config")
    pictureMatchConfig.dataSource = dataSourceStorage.pictureDataSource
    pictureMatchConfig.analyser = new PictureHashAnalyser2(dataSourceStorage.pictureHashDataSource)
    pictureMatchConfig.matcher = new Level2PictureMatcher(matchSaverPipeline.asDataSource("filter_output", MatchCandidateWithoutTypeDfType))
    pictureMatchConfig.scorer = new PictureScorer(true)
    pictureMatchConfig.matchEntityType = PictureType

    //add stage
    picturePipeline.addStage(picturePipelineInitialisationStage)
    picturePipeline.addStage(picturePipelineAnalyserStage)
    picturePipeline.addStage(picturePipelineMatcherStage)
    picturePipeline.addStage(picturePipelineScoringStage)
    picturePipeline.addStage(picturePipelineNormalisationStage)
    picturePipeline.addStage(picturePipelinePersisterStage)
    picturePipeline.addStage(picturePipelineTrainingDataExtractionStage)
    picturePipeline.addStage(picturePipelineStatisticStage)

    picturePipeline.addConfig(pictureMatchConfig)

    //run pipeline
    picturePipeline.run()

    DbManager.saveMatchInfo("second_level_end_date", LocalDateTime.now().toString)

    sparkSession.stop()
  }

  def runOptimizer = {
    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Optimiser")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    import sparkSession.implicits._

    //create data source storage
    val dataSourceStorage2 = new DataSourceStorage(sparkSession)

    //create pipeline
    val optimizerPipeline = new Pipeline("optimizer_pipeline")

    //configure pipeline
    val optimizerConfig = new PipelineConfig("optimizer config")
    optimizerConfig.dataSource = dataSourceStorage2.matchCandidateDataSource

    //define error column
    //    val errorColumn = $"sum".cast(DoubleType)
    val errorColumn = (lit(1.0) / ($"top5" + 1)).cast(DoubleType)


    optimizerPipeline.addConfig(optimizerConfig)

    val optInitialisationStage = new InitialisationStage(Nil, "init_output")
    val optWeightTrainerStage = new WeightTrainerStage("init_output" :: Nil, "weight", ProgramConfig.optimizerWindowsSize, ProgramConfig.optimizerAreaNumberToEvaluate, errorColumn, dataSourceStorage2.trainingPairDataSource)
    val optTotalStage = new TotalStatisticStage("weight" :: Nil, "total", dataSourceStorage2.matchCandidateDataSource, dataSourceStorage2.typeDataSource)

    optWeightTrainerStage.addType(PersonType)
    optWeightTrainerStage.addType(FormulaType)
    optWeightTrainerStage.addType(TextType)
    optWeightTrainerStage.addType(CosineTitleType)
    optWeightTrainerStage.addType(AbstractType)
    optWeightTrainerStage.addType(PictureType)
    optWeightTrainerStage.addType(InstrumentType)

    optimizerPipeline.addStage(optInitialisationStage)
    optimizerPipeline.addStage(optWeightTrainerStage)
    optimizerPipeline.addStage(optTotalStage)


    //run pipeline
    optimizerPipeline.run()

    DbManager.saveMatchInfo("optimizer_end_date", LocalDateTime.now().toString)

    sparkSession.stop()
  }

  def runWordSpec = {
    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordSpec")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    //create data source storage
    val dataSourceStorage = new DataSourceStorage(sparkSession)

    //create stage
    val initialisationStage = new InitialisationStage(Nil, "init")
    val analyserStage = new AnalyserStage("init" :: Nil, "analyzer_output")

    //create pipeline
    val wordSpecPipeline = new Pipeline("word_spec_pipeline")
    wordSpecPipeline.addStage(initialisationStage)
    wordSpecPipeline.addStage(analyserStage)

    //configure pipeline
    val wordSpecConfig = new PipelineConfig("word spec config")
    wordSpecConfig.dataSource = dataSourceStorage.fileDataSource
    wordSpecConfig.matchEntityType = NoType
    wordSpecConfig.analyser = new DocumentWordSpecAnalyser

    //add config
    wordSpecPipeline.addConfig(wordSpecConfig)

    //run
    wordSpecPipeline.run()
  }

}
