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
package eu.ill.puma.sparkmatcher.test.keyword

import eu.ill.puma.sparkmatcher.matching.datasource.DataSourceStorage
import eu.ill.puma.sparkmatcher.test.clustering.ClusteringUtils.{array_distinct, processInputDataMapFunction, schema}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.log4j.Level
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{SaveMode, SparkSession}

object FPGrowthExample {


  def run = {
    //startup
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)


    /**
      * spark setup
      */

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


    /**
      * process input data
      */

    //    val wordsData = dataSourceStorage.titleDataSource.loadData()._2
    //      .limit(1000)
    //      .select($"entity" as "text", $"document_version_id" as "id")
    //      .map(processInputDataMapFunction, RowEncoder.apply(schema))
    //      .select(array_distinct($"words") as "words", $"id")


    val wordsData = dataSourceStorage.abstractDataSource.loadData()._2
      .select($"text", $"document_version_id" as "id")
      .map(processInputDataMapFunction, RowEncoder.apply(schema))
      .select(array_distinct($"words") as "words", $"id")

    val fpgrowth = new FPGrowth()
      .setItemsCol("words")
      .setMinSupport(0.1)
      .setMinConfidence(0.3)


    val model = fpgrowth.fit(wordsData)

    // Display frequent itemsets.
    val itemFrequency = model.freqItemsets

    // Display generated association rules.
    val associationRules = model.associationRules

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    val result = model.transform(wordsData)

    //save to db
    itemFrequency.write.mode(SaveMode.Overwrite).jdbc(ProgramConfig.matchingDatabaseUrl, "fpgrowth_item_frequency", ProgramConfig.dbProperties)
    associationRules.write.mode(SaveMode.Overwrite).jdbc(ProgramConfig.matchingDatabaseUrl, "fpgrowth_association_rules", ProgramConfig.dbProperties)
    result.write.mode(SaveMode.Overwrite).jdbc(ProgramConfig.matchingDatabaseUrl, "fpgrowth_result", ProgramConfig.dbProperties)


  }
}
