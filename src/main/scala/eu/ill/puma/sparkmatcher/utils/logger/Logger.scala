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
package eu.ill.puma.sparkmatcher.utils.logger

import java.io.FileWriter
import java.util.Calendar

object Logger {

  def levelColumnSize = 5

  def classNameColumnSize = 35

  def entityTypeColumnString = 25

  val startTime = System.currentTimeMillis()

  var errorOccured = false

  //  def time(className: String, module: String, message: String) = {
  //    this.log("TIME ", Console.CYAN, className, module, message)
  //  }

  def info(className: String, module: String, message: String) = {
    this.log("INFO ", Console.GREEN, className, module, message)
  }

  def warn(className: String, module: String, message: String) = {
    this.log("WARN ", Console.YELLOW, className, module, message)
  }

  def error(className: String, module: String, message: String) = {
    this.log("ERROR", Console.RED, className, module, message)
    errorOccured = true
  }

  def log(level: String, levelColor: String, className: String, module: String, message: String) = {
    var logString: String = ""
    val time = Calendar.getInstance().getTime().toString
    logString += this.loggify(time, Console.BLACK, time.length)
    logString += this.loggify(((System.currentTimeMillis - startTime) / 1000).toString, Console.BLACK, 5)
    logString += this.loggify(level, levelColor, levelColumnSize)
    logString += this.loggify(className, Console.MAGENTA, classNameColumnSize)
    logString += this.loggify(module, Console.BLUE, entityTypeColumnString)

    val fw = new FileWriter("log.txt", true)
    try {
      fw.write(s"$logString : $message\n".replaceAll("\u001B\\[[;\\d]*m", ""))
    }
    finally fw.close()

    println(s"$logString : $message")
  }

  def loggify(content: String, color: String, columnSize: Int): String = {
    val contentLength = content.length
    var formatedContent = "[" + color

    if (contentLength > columnSize) {
      formatedContent += content.substring(0, columnSize - 1)
    } else {
      formatedContent += content
      for (i <- (1 until columnSize - contentLength)) {
        formatedContent += " "
      }
    }

    formatedContent = formatedContent + Console.BLACK + "]"

    formatedContent
  }
}
