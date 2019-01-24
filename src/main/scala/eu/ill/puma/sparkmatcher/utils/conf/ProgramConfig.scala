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
package eu.ill.puma.sparkmatcher.utils.conf

import java.util.Properties

import eu.ill.puma.sparkmatcher.utils.logger.Logger
import org.apache.spark.SparkConf

import scala.io.Source
import scala.util.matching.Regex

object ProgramConfig extends Serializable {

  //init
  private var properties = new Properties()

  var prod = false
  val environment = System.getProperty("puma.matching.environment")

  init

  //database
  val startSchema: String = getStringProperty("puma.persistence.database.matching.startSchemaName")

  val finalSchema: String = getStringProperty("puma.persistence.database.matching.finalSchemaName")

  val corpusDatabaseUrl: String = getStringProperty("puma.persistence.database.corpus.url")

  val matchingDatabaseUrl: String = getStringProperty("puma.persistence.database.matching.url") + "?currentSchema=" + startSchema

  val driver: String = getStringProperty("puma.persistence.database.driver")

  val username: String = getStringProperty("puma.persistence.database.username")

  val password: String = getStringProperty("puma.persistence.database.password")

  val dbProperties: Properties = {
    val dbProperties = new Properties

    dbProperties.put("user", username)
    dbProperties.put("password", password)
    dbProperties.put("driver", driver)

    dbProperties
  }

  //file
  val fileRoot: String = getStringProperty("puma.persistence.file.root")
  val sparkResourceRoot: String = getStringProperty("puma.persistence.resource.root")

  //text matching
  val stopwords: List[String] = Source.fromURL(getClass.getResource("/stopwords.txt")).mkString.split("\n").toList

  val minimumWordLength: Int = getIntProperty("puma.matching.textmatcher.word.minLength")
  val sentenceLength: Int = getIntProperty("puma.matching.textmatcher.sentence.minLength")
  val maximumSentenceOccurency: Int = getIntProperty("puma.matching.textmatcher.sentence.maxoccurency")
  val matchingMinScore: Int = getIntProperty("puma.matching.textmatcher.minScore")

  //entities matching
  val maximumEntitiesOccurency: Int = getIntProperty("puma.matching.entitiesmatcher.occurrence.maximum")
  val scoreFactor: Int = getIntProperty("puma.matching.scoreFactor")

  //cosine matching
  val cosineMinScore: Double = getDoubleProperty("puma.matching.cosine.minScore")

  //global settings
  val partitionNumber: Int = getIntProperty("puma.matching.partition")
  val deduplicationPartitionNumber: Int = getIntProperty("puma.deduplication.partition")

  //normalisation
  val normalisationPercentile: Int = getIntProperty("puma.matching.normalisationPercentile")
  val normalisationValue: Int = getIntProperty("puma.matching.normalisationValue")

  //rareWord extraction
  val maximumRareWordCount: Int = getIntProperty("puma.matching.maximumRareWordCount")

  val minimumRareWordTf: Int = getIntProperty("puma.matching.minimumRareWordTf")

  //title matching
  val titleMinimumMatchedWords: Int = getIntProperty("puma.matching.title.minimumMatchedWords")
  val titleMinimumWordLength: Int = getIntProperty("puma.matching.title.minimumWordLength")

  //picture matching
  val pictureMinHeight: Int = getIntProperty("puma.matching.picture.minheight")
  val pictureMinWidth: Int = getIntProperty("puma.matching.picture.minwidth")
  val pictureApproxThreshold: Double = getDoubleProperty("puma.matching.picture.approxThreshold")
  val pictureThreshold: Int = getIntProperty("puma.matching.picture.threshold")
  val pictureVectorSize: Int = getIntProperty("puma.matching.picture.vector.size")

  //optimizer
  val optimizerAreaNumberToEvaluate = getIntProperty("puma.optimizer.areaNumberToEvaluate")
  val optimizerWindowsSize = getIntProperty("puma.optimizer.windowsSize")
  val optimizerDoiWeight = getDoubleProperty("puma.optimizer.doiWeight")
  val optimizerProposalCodeWeight = getDoubleProperty("puma.optimizer.proposalCodeWeight")

  //nlp
  val wordSpecMinFrequency = getIntProperty("puma.nlp.wordSpec.minFrequency")

  //instrument analysis
  val maximumInstrumentFrequency = getDoubleProperty("puma.analysis.instrument.maxfrequency")

  //spark
  val defaultSparkConfig: SparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", "file:/tmp/spark-events")
    .set("spark.history.fs.logDirectory", "file:/tmp/spark-events")
    .set("spark.kryoserializer.buffer.max", "2047m")
    .set("spark.driver.maxResultSize", "8g")
    .set("spark.executor.memory", "120g")
    .set("spark.executor.cores", "32")
    //    .set("spark.memory.offHeap.enabled", "true")
    //    .set("spark.memory.offHeap.size", "120g")
    .set("spark.driver.memory", "32g")
    .set("spark.driver.cores", "16")
    .set("spark.rpc.lookupTimeout", "720000")
    .set("spark.rpc.askTimeout", "720000")
    .set("spark.sql.broadcastTimeout", "720000")
    .set("spark.network.timeout", "720000")
    .set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "1024")
    .set("spark.sql.shuffle.partitions", ProgramConfig.partitionNumber.toString)
    .set("spark.debug.maxToStringFields", "200")
  //    .set("spark.executor.cores", "128")

  var env = ""

  def init: Unit = {

    var configFileName = ""
    if (environment == "prod") {
      configFileName = "/config-prod.properties"
      prod = true
    } else {
      configFileName = "/config-test.properties"
    }

    Logger.info("App", "no config", s"Config file name : $configFileName")


    val url = getClass.getResource(configFileName)

    if (url != null) {
      properties.load(Source.fromURL(url).bufferedReader())
    } else {
      throw new RuntimeException("impossible to load conf from " + configFileName)
    }
  }

  /**
    * Read a property value and determine if it matches a ${sys-var-name}:default-value structure
    *
    * @return the system variable value or default value or simple property value
    */
  def getStringProperty(propertyName: String): String = {
    val pattern: Regex = ("\\$\\{([0-9a-zA-Z_]+)\\}:([^\\n\\r]+)").r
    val propertyValue = properties.getProperty(propertyName)

    if (propertyValue == null) {
      println("Couldn't find property \"" + propertyName + "\"")
      ""

    } else {
      // Check for matching
      val patternMatch = pattern.findFirstMatchIn(propertyValue)
      if (!patternMatch.isEmpty) {
        val systemPropertyName = patternMatch.get.group(1)
        val defaultValue = patternMatch.get.group(2)

        // Get system variable value
        if (sys.env.contains(systemPropertyName)) {
          // Return system variable value
          sys.env(systemPropertyName)

        } else {
          // Return default value
          defaultValue
        }

      } else {
        // Return simple property value
        propertyValue
      }
    }
  }

  def getIntProperty(propertyName: String): Int = {
    getStringProperty(propertyName).toInt
  }

  def getBooleanProperty(propertyName: String): Boolean = {
    getStringProperty(propertyName).toBoolean
  }

  def getDoubleProperty(propertyName: String): Double = {
    getStringProperty(propertyName).toDouble
  }

}