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
package eu.ill.puma.sparkmatcher.matching.training

import eu.ill.puma.sparkmatcher.matching.pipepline.{EntityType, Pipeline}
import eu.ill.puma.sparkmatcher.utils.logger.Logger

import scala.collection.mutable.ListBuffer

class Trainer(pipeline: Pipeline, evaluator: Evaluator, entityType: EntityType, maxIteration: Int = 100) {

  private var trainArgs = new ListBuffer[(Double => Unit, Double, Double, Double, String)]

  def addTrainingFunction(trainingFunction: Double => Unit, start: Double, end: Double, minRange: Double, name: String) = {
    trainArgs.append((trainingFunction, start, end, minRange, name))
  }

  def train = {
    //init parameter to their average value
    trainArgs.foreach(trainArg => initParameter(trainArg._1, trainArg._2, trainArg._3, trainArg._5))

    val paramResult = ListBuffer.empty[(String, Double)]

    //foreach function and parameter set
    trainArgs.foreach(trainArg => {
      val result = dichotomy(trainArg._1, trainArg._2, trainArg._3, trainArg._4)

      paramResult.append((trainArg._5, result))
    })

    paramResult.foreach(result => {
      Logger.info("trainer", entityType.stringValue, result.toString())
    })
  }

  def initParameter(trainingFunction: Double => Unit, start: Double, end: Double, name: String) = {
    trainingFunction((end + start) / 2)
    Logger.info("trainer", entityType.stringValue, s"init $name to ${(end + start) / 2}")
  }

  def dichotomy(trainingFunction: Double => Unit, start: Double, end: Double, minRange: Double): Double = {
    //init
    var pivot = (end + start) / 2
    var lowerBound = start
    var upperBound = end
    var counter = 0

    while ((upperBound - lowerBound) / 2 >= minRange) {
      //value to test
//      val rightValue = (lowerBound + pivot) / 2
//      val leftValue = (upperBound + pivot) / 2
//
//      //monitoring info
//      counter += 1;
//      Logger.warn("trainer", entityType.stringValue, s"start iteration : $counter, pivot = $pivot, lowerBound = $lowerBound, upperBound = $upperBound, rightValue = $rightValue, leftValue = $leftValue")
//
//      //run right
//      trainingFunction(rightValue)
//      DbManager.fullReset
//      pipeline.reset
//      val rightResult = evaluator.run(pipeline.run.toList(0)._4)
//
//      //run left
//      trainingFunction(leftValue)
//      DbManager.fullReset
//      pipeline.reset
//      val leftResult = evaluator.run(pipeline.run.toList(0)._4)
//
//      Logger.warn("trainer", entityType.stringValue, s"end iteration : $counter, pivot = $pivot, lowerBound = $lowerBound, upperBound = $upperBound, rightResult = $rightResult, leftResult = $leftResult")
//
//      //new pivot
//      pivot = if (evaluator.lowerIsBetter) {
//        if (rightResult < leftResult) rightValue else leftValue
//      } else {
//        if (rightResult > leftResult) rightValue else leftValue
//      }
//
//      //new bound
//      lowerBound = pivot - ((upperBound - lowerBound) / 4)
//      upperBound = pivot + ((upperBound - lowerBound) / 4)
    }

    //final result
    pivot
  }

}
