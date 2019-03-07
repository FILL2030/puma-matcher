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

import eu.ill.puma.sparkmatcher.matching.analyser._
import eu.ill.puma.sparkmatcher.matching.datasource.DataSourceStorage
import eu.ill.puma.sparkmatcher.matching.filter._
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.matching.stage._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object TestApp {
  def main(args: Array[String]): Unit = {


    //    this.runDoi
//        this.test

    this.testScoreList
  }


  def testAdvTechnics(): Unit = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)

    Logger.info("App", "database", s"running on database : ${ProgramConfig.matchingDatabaseUrl}")

    DbManager.resetWordPosTag()
    DbManager.resetTechnique()

    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Matcher")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    //create data source storage
    val dataSourceStorage = new DataSourceStorage(sparkSession)

    //create pipeline
    val pipeline = new Pipeline("test pipeline")

    //configure pipeline
    val testConfig = new PipelineConfig("test config")
    testConfig.dataSource = dataSourceStorage.fileDataSource
    testConfig.matchEntityType = ScientificTechniqueType
    testConfig.analyser = new TextEntitiesAnalyser(dataSourceStorage.typeDataSource, dataSourceStorage.techniqueDataSource :: Nil)


    //add config
    pipeline.addConfig(testConfig)

    val initialisationStage = new InitialisationStage(Nil, "init_output")

    val analyserStage = new AnalyserStage("init_output" :: Nil, "analyzer_output")

    val filterStage = new FilterStage("analyzer_output" :: Nil, "filter_output")
    filterStage.addFilter(new InstrumentTechniqueFilter(dataSourceStorage.instrumentTechnicsDataSource))
    filterStage.addFilter(new PersonTechniqueFilter(dataSourceStorage.personDataSource))

    pipeline.addStage(initialisationStage)
    pipeline.addStage(analyserStage)
    pipeline.addStage(filterStage)

    //run
    pipeline.run

  }


  def testAdvInstrument(): Unit = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
    //    DbManager.fullReset

    Logger.info("App", "database", s"running on database : ${ProgramConfig.matchingDatabaseUrl}")

    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Matcher")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    //create data source storage
    val dataSourceStorage = new DataSourceStorage(sparkSession)

    //create pipeline
    val pipeline = new Pipeline("test pipeline")

    //configure pipeline
    val testConfig = new PipelineConfig("test config")
    testConfig.dataSource = dataSourceStorage.fileDataSource
    testConfig.analyser = new AdvancedInstrumentAnalyser(dataSourceStorage.instrumentDataSource, dataSourceStorage.instrumentNameDataSource, dataSourceStorage.instrumentAliasDataSource, dataSourceStorage.typeDataSource, dataSourceStorage.trainingDataDataSource, "analysed_instrument")
    //    testConfig.matcher = new EntitiesMatcherV2
    //    testConfig.scorer = new EntitiesScorer
    testConfig.matchEntityType = InstrumentType

    //add config
    pipeline.addConfig(testConfig)

    val initialisationStage = new InitialisationStage(Nil, "init_output")

    val analyserStage = new AnalyserStage("init_output" :: Nil, "analyzer_output")

    val matcherStage = new MatcherStage("analyzer_output" :: Nil, "matcher_output")

    val filterStage = new FilterStage("matcher_output" :: Nil, "filter_output")
    filterStage.addFilter(new TypeFilter(dataSourceStorage.typeDataSource))
    filterStage.addFilter(new DocumentVersionIdFilter(dataSourceStorage.validDocumentVersionIdDataSource))
    filterStage.addFilter(new DateFilter(dataSourceStorage.dateDateSource, dataSourceStorage.documentVersionSourceDataSource))

    val scoringStage = new ScoringStage("filter_output" :: Nil, "scorer_output")

    val countStage = new CountStage("scorer_output" :: Nil, "")

    pipeline.addStage(initialisationStage)
    pipeline.addStage(analyserStage)

    //run
    pipeline.run

  }

  def testPicture(): Unit = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
    DbManager.fullReset

    Logger.info("App", "database", s"running on database : ${ProgramConfig.matchingDatabaseUrl}")

    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Matcher")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    //create data source storage
    val dataSourceStorage = new DataSourceStorage(sparkSession)

    //create pipeline
    val pipeline = new Pipeline("test pipeline")

    //configure pipeline
    val testConfig = new PipelineConfig("test config")
    testConfig.dataSource = dataSourceStorage.pictureDataSource
    testConfig.analyser = new PictureHashAnalyser2(dataSourceStorage.pictureHashDataSource)
    testConfig.matchEntityType = PictureType

    //add config
    pipeline.addConfig(testConfig)

    val initialisationStage = new InitialisationStage(Nil, "init_output")

    val analyserStage = new AnalyserStage("init_output" :: Nil, "analyzer_output")

    val viewStage = new ViewStage("analyzer_output" :: Nil, "", 20)

    pipeline.addStage(initialisationStage)
    pipeline.addStage(analyserStage)
    pipeline.addStage(viewStage)

    //run
    pipeline.run

    RankEvaluatorApp.run

  }

  def runWordSpec = {
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)

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
    wordSpecConfig.analyser = new DocumentWordSpecAnalyser2

    //add config
    wordSpecPipeline.addConfig(wordSpecConfig)

    //run
    wordSpecPipeline.run()
  }


  def runDoi = {
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)

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
    val doiMatchConfig = new PipelineConfig("word spec config")
    doiMatchConfig.matchEntityType = DoiType
    doiMatchConfig.dataSource = dataSourceStorage.doiDataSource
    doiMatchConfig.analyser = new DoiAnalyser(dataSourceStorage.fileDataSource)
    doiMatchConfig.analyser = new DoiAnalyser(dataSourceStorage.fileDataSource)

    //add config
    wordSpecPipeline.addConfig(doiMatchConfig)

    //run
    wordSpecPipeline.run()
  }

  def test = {
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)

    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordSpec")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    val hdfs = sparkSession.read.textFile("hdfs://puma-spark-1:9000/user/hadoop/ngrams/*")

    Logger.info("App", "no config", "start")

    println(hdfs.count())

    Logger.info("App", "no config", "stop")

    //    sparkSession.sparkContext.textFile("hdfs://puma-spark-1:9000/user/hadoop/ngrams/*").toDF().show(20)
  }

  def testScoreList = {
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)


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
    val scoreListStage = new ScoreListStage("init" :: Nil, "output",  dataSourceStorage.trainingPairDataSource)

    //create pipeline
    val pipeline = new Pipeline("test_pipeline")
    pipeline.addStage(initialisationStage)
    pipeline.addStage(scoreListStage)

    //configure pipeline
    val testConfig = new PipelineConfig("test config")
    testConfig.dataSource = dataSourceStorage.matchCandidateDataSource

    //add config
    pipeline.addConfig(testConfig)

    //run
    pipeline.run()


  }
}
