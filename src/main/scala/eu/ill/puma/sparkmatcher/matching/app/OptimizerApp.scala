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

import eu.ill.puma.sparkmatcher.matching.datasource.DataSourceStorage
import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.matching.stage.{InitialisationStage, _}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object OptimizerApp {


  def main(args: Array[String]): Unit = {

    this.run()

  }

  def run(): Unit = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
    DbManager.resetTotalMatchCandidateStats()
    DbManager.resetWeight()

    Logger.info("App", "database", s"running on database : ${ProgramConfig.matchingDatabaseUrl}")

    //create spark session
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Matcher")
      .config(ProgramConfig.defaultSparkConfig)
      .getOrCreate()

    import sparkSession.implicits._

    //create data source storage
    val dataSourceStorage = new DataSourceStorage(sparkSession)

    //create pipeline
    val pipeline = new Pipeline("optimizer_pipeline")

    //configure pipeline
    val testConfig = new PipelineConfig("optimizer config")
    testConfig.dataSource = dataSourceStorage.matchCandidateDataSource

    //define error column
    //    val errorColumn = $"sum".cast(DoubleType)
    val errorColumn = (lit(1.0) / ($"top5" + 1)).cast(DoubleType)


    pipeline.addConfig(testConfig)

    val initialisationStage = new InitialisationStage(Nil, "init_output")
    val weightTrainerStage = new WeightTrainerStage("init_output" :: Nil, "weight", ProgramConfig.optimizerWindowsSize, ProgramConfig.optimizerAreaNumberToEvaluate, errorColumn, dataSourceStorage.trainingPairDataSource)
    val totalStage = new TotalStatisticStage("weight" :: Nil, "total", dataSourceStorage.matchCandidateDataSource, dataSourceStorage.typeDataSource)

    weightTrainerStage.addType(PersonType)
    weightTrainerStage.addType(FormulaType)
    weightTrainerStage.addType(CosineTitleType)
    weightTrainerStage.addType(TextType)
    weightTrainerStage.addType(InstrumentType)
    weightTrainerStage.addType(MatcherNumberType)
    weightTrainerStage.addType(PictureType)
    weightTrainerStage.addType(AbstractType)
    //    weightTrainerStage.addType(TitleType)

    pipeline.addStage(initialisationStage)
    pipeline.addStage(weightTrainerStage)
    pipeline.addStage(totalStage)

    //run pipeline
    pipeline.run()
  }
}
