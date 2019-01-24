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
package eu.ill.puma.sparkmatcher.test

import eu.ill.puma.sparkmatcher.matching.analyser.DocumentWordSpecAnalyser
import eu.ill.puma.sparkmatcher.matching.datasource.DataSourceStorage
import eu.ill.puma.sparkmatcher.matching.pipepline.{NoType, Pipeline, PipelineConfig}
import eu.ill.puma.sparkmatcher.matching.stage.{AnalyserStage, InitialisationStage}
import eu.ill.puma.sparkmatcher.test.clustering.LDAWordSpecExample
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

import scala.language.postfixOps

object TestMl {
  def main(args: Array[String]): Unit = {

//    TestMl.test

    LDAWordSpecExample.run

//    KMeansTitleTfKeywordExample.run
    //FPGrowthExample.run
    //KMeansTitleWord2VecExample.run
    //KMeansTitleTfIdfExample.run
    //PICExample.run

    //end


  }


  def test = {

    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)

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

    //create config
    val testConfig = new PipelineConfig("test config")
    testConfig.dataSource = dataSourceStorage.fileDataSource
    testConfig.matchEntityType = NoType
    testConfig.analyser = new DocumentWordSpecAnalyser

    val initialisationStage = new InitialisationStage(Nil, "init")

    val analyserStage = new AnalyserStage("init" :: Nil, "analyzer_output")

    //create pipepline
    val testPipeline = new Pipeline("test ML pipeline")
    testPipeline.addConfig(testConfig)
    testPipeline.addStage(initialisationStage)
    testPipeline.addStage(analyserStage)

    //run pipeline
    testPipeline.run()

    Logger.info("App", "no config", "Pipeline complete")

  }

}

