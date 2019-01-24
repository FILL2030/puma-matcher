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
package eu.ill.puma.sparkmatcher.matching.datasource

import java.util.Properties

import eu.ill.puma.sparkmatcher.matching.pipepline.DataFrameType
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class SqlDataSource(sparkSession: SparkSession,
                    var sql: String,
                    var dataType: DataFrameType,
                    var name: String,
                    var partitionCol: Option[String] = None,
                    var partitionNumber: Int = ProgramConfig.partitionNumber,
                    var dataBaseUrl: String = ProgramConfig.corpusDatabaseUrl,
                    var dbProperties: Properties = ProgramConfig.dbProperties) extends DataSource {
  def loadData(): (DataFrameType, DataFrame) = {
    if (this.dataFrame.isEmpty) {
      if (partitionCol.isDefined) {

        val upperboundSql = s"select max(${partitionCol.get}) from $sql"
        val upperbound = DbManager.runIntQuery(upperboundSql, dataBaseUrl)


        this.dataFrame = Some(sparkSession.read
          .option("partitionColumn", partitionCol.get)
          .option("lowerBound", 0)
          .option("upperBound", upperbound)
          .option("numPartitions", partitionNumber)
          .jdbc(dataBaseUrl, sql, dbProperties)
          .cache())
      } else {
        this.dataFrame = Some(sparkSession.read
          .jdbc(dataBaseUrl, sql, dbProperties)
          .repartition(partitionNumber)
          .cache())
      }
    }

    (dataType, this.dataFrame.get)
  }

  def partitionColumn: Option[String] = partitionCol

  private var dataFrame: Option[DataFrame] = None
}

