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

import eu.ill.puma.sparkmatcher.matching.pipepline.{DataFrameType, TextDfType}
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.database.DbManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

class FileDataSource(sparkSession: SparkSession,
                     var sql: String,
                     var name : String,
                     var partitionNumber: Int = ProgramConfig.partitionNumber,
                     var dataBaseUrl: String = ProgramConfig.corpusDatabaseUrl,
                     var dbProperties: Properties = ProgramConfig.dbProperties,
                     var fileRoot: String = ProgramConfig.fileRoot) extends DataSource {

  private var fileDataFrame: Option[DataFrame] = None

  override def loadData: (DataFrameType, DataFrame) = {
      if (this.fileDataFrame.isEmpty) {
        import sparkSession.implicits._

        val upperboundSql = s"select max(document_version_id) from $sql"
        val upperBound = DbManager.runIntQuery(upperboundSql, dataBaseUrl)

        //read db
        var dataFromDb = sparkSession.read
          .option("partitionColumn", "document_version_id")
          .option("lowerBound", 1)
          .option("upperBound", upperBound)
          .option("numPartitions", partitionNumber).jdbc(dataBaseUrl, sql, dbProperties)

        //load file
        val fileDataSet = dataFromDb.map(x =>
          (x.getLong(0), Source.fromFile(fileRoot + "/" + x.getString(1)).mkString, x.getString(1))
        )

        //return
        this.fileDataFrame = Some(fileDataSet.toDF("document_version_id", "text", "file_path").cache())
      }

    (TextDfType, this.fileDataFrame.get)
  }
}
