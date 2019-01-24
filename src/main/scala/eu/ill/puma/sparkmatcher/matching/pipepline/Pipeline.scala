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
package eu.ill.puma.sparkmatcher.matching.pipepline

import eu.ill.puma.sparkmatcher.matching.datasource.PipelineDataSource
import eu.ill.puma.sparkmatcher.matching.stage.Stage
import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class Pipeline(val name: String) {

  val stages = ListBuffer.empty[Stage]

  private var configs = ListBuffer.empty[PipelineConfig]

  private var dataFrameStorage = ListBuffer.empty[(PipelineConfig, String, DataFrameType, DataFrame)]

  private var completedMatcher = 0

  def addStage(stage: Stage) = synchronized {
    var outputAlreadyExist: Boolean = false

    stages.foreach(alreadyREgisteredStage => {
      if (alreadyREgisteredStage.output == stage.name) {
        outputAlreadyExist = true
      }
    })

    if (outputAlreadyExist) {
      throw new Exception(s"The stage : '${stage.name.stringValue} output already exist")
    } else {
      stages.append(stage)
    }
  }

  def addConfig(matchBuilder: PipelineConfig) = synchronized {
    configs.append(matchBuilder)
  }

  def asDataSource(config: PipelineConfig, dfOrigin: String, dataFrameType: DataFrameType) = synchronized {
    val result = dataFrameStorage
      .filter(_._1 == config)
      .filter(_._2 == dfOrigin)
      .filter(_._3 == dataFrameType)
      .map(x => x._4)
      .head

    new PipelineDataSource(dataFrameType, result)
  }

  def asDataSource(dfOrigin: String, dataFrameType: DataFrameType): PipelineDataSource = synchronized {
    val dataFrameToMerge: List[DataFrame] = dataFrameStorage
      .filter(_._2 == dfOrigin)
      .filter(_._3 == dataFrameType)
      .map(x => x._4)
      .toList

    var result: DataFrame = null
    var fieldNames = Array[String]()

    dataFrameToMerge.foreach(dataFrame =>
      if (result == null) {
        fieldNames = dataFrame.columns

        val sortedDf: DataFrame = dataFrame.select(fieldNames.head, fieldNames.tail: _*)

        result = sortedDf

      } else {
        val sortedDf: DataFrame = dataFrame.select(fieldNames.head, fieldNames.tail: _*)

        result = result union sortedDf
      })

    new PipelineDataSource(dataFrameType, result)
  }

  def run() =  {

    //launch match builders async
    configs.foreach(config => {
      val future = Future {

        stages.foreach(stage => {

          //setup
          val foundInputs = ListBuffer.empty[String]

          //get data required by the stage
          val inputData = dataFrameStorage
            .filter(_._1 == config)
            .filter(x => stage.input.contains(x._2))
            .map(row => {
              foundInputs.append(row._2)
              (row._3, row._4)
            }).toList


          Thread.sleep(2000)

          //check if stage is optional
          if (stage.isOptional(config)) {
            this.skipStage(inputData, stage, config)
          }

          //check stage input number
          else if (stage.input.size > stage.maxInputNumber(config)) {
            Logger.error(stage.name.stringValue, config.name, s"Invalid input number for stage, found : ${stage.input.size}, maximum : ${stage.maxInputNumber(config)}")

            throw new Exception(s"The stage : '${stage.name.stringValue} (${config.name})' has too much inputs : !")
          }

          //check dependency are resolved and run the stage
          else if (foundInputs.forall(a => stage.input.contains(a)) && foundInputs.size == stage.input.size) {

            //assert the stage accept the input
            val areInputsValid = foundInputs.forall(a => stage.validInputType(config).contains(a))

            if (stage.acceptAnyInput == false && areInputsValid) {
              Logger.error(stage.name.stringValue, config.matchEntityType.get.stringValue, s"The stage : '${stage.name.stringValue} (${config.name})' has receive a unsuported dataframe type !")

              throw new Exception(s"The stage : '${stage.name.stringValue} (${config.name})' has receive a unsuported dataframe type !")
            }

            this.runStage(inputData, stage, config)

          }

          //error management
          else {
            Logger.error(stage.name.stringValue, config.name, s"Missing dependancy, found : ${foundInputs.mkString(", ")}, required : ${stage.input.mkString(", ")}")

            throw new Exception(s"The stage : '${stage.name.stringValue} (${config.name})' has missing dependancy : !")
          }
        })
      }

      future.onComplete {
        case Success(value) => {
          Logger.info("pipeline", config.name, s"Complete matcher")
          completedMatcher = completedMatcher + 1
        }
        case Failure(e) => {
          Logger.error(s"pipeline", config.name, s"Config failed")
          Thread.sleep(30)
          e.printStackTrace()
          completedMatcher = completedMatcher + 1
        }
      }
    })


    while (completedMatcher < configs.size) {
      Thread.sleep(1000)
    }
  }


  def runStage(inputData: List[(DataFrameType, DataFrame)], stage: Stage, config: PipelineConfig) = {
    //initial logging
    val start = System.currentTimeMillis()
    Logger.info(stage.name.stringValue, config.name, "Start stage")

    //run the stage
    val stageResult = stage.run(config, inputData)

    //check that the stage have produce some data
    if (stage.produceData(config)) {
      assert(stageResult.isDefined, s"The stage : '${stage.name.stringValue} (${config.name})' has produce no data !")

      //extract result
      val (resultType, resultDataFrame) = stageResult.get

      //validate result
      if (DataFrameValidator.isCompliantToSchema(resultDataFrame, resultType.schema) == false) {
        Logger.error(stage.name.stringValue, config.name, s"The stage : '${stage.name.stringValue} (${config.name})' has produce a non valid dataframe !")
        Logger.error(stage.name.stringValue, config.name, "found schema : ")
        resultDataFrame.printSchema()
        Logger.error(stage.name.stringValue, config.name, "expected schema : ")
        resultType.schema.printTreeString()

        throw new Exception(s"The stage : '${stage.name.stringValue} (${config.name})' has produce an invalid dataframe !")
      }
      //cache result
      val cachedResultDataFrame = resultDataFrame.cache()

      //save result
      synchronized {
        dataFrameStorage.append((config, stage.output, resultType, cachedResultDataFrame))
      }
    }

    //final logging
    Logger.info(stage.name.stringValue, config.name, s"Stage completed in ${(System.currentTimeMillis() - start) / 1000} s")
  }

  def skipStage(inputData: List[(DataFrameType, DataFrame)], stage: Stage, config: PipelineConfig) = synchronized {
    Logger.info(stage.name.stringValue, config.name, "Skip optional stage")

    //Passtrough stage data to next stage
    inputData.foreach(input => {
      dataFrameStorage.append((config, stage.output, input._1, input._2))
    })
  }

  def reset = synchronized {
    Logger.info("pipeline", "pipeline", "Reset pipeline")
    completedMatcher = 0
    dataFrameStorage = ListBuffer.empty[(PipelineConfig, String, DataFrameType, DataFrame)]
  }

  def fullReset = synchronized {
    Logger.info("pipeline", "pipeline", "Reset pipeline (full)")
    completedMatcher = 0
    dataFrameStorage = ListBuffer.empty[(PipelineConfig, String, DataFrameType, DataFrame)]
    configs = ListBuffer.empty[PipelineConfig]
  }

  def fork: Pipeline = synchronized {
    val forkedPipeline = new Pipeline("forked" + this.name)

    forkedPipeline.stages.appendAll(this.stages)
    forkedPipeline.configs.appendAll(this.configs)
    forkedPipeline.completedMatcher = this.completedMatcher
    forkedPipeline.dataFrameStorage.appendAll(this.dataFrameStorage)

    forkedPipeline
  }
}
