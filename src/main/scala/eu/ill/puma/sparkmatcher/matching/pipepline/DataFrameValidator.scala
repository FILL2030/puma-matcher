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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

object DataFrameValidator {
  def hasColumn(dataFrame: DataFrame, columnType: StructField): Boolean = {
    var result = false

    //get schema
    val schema = dataFrame.schema

    //check is the column exist
    if (schema.fieldNames.toList.contains(columnType.name)) {
      val columnPosition = schema.fieldIndex(columnType.name)

      val foundColumn = schema.fields(columnPosition)

      result = (foundColumn.dataType == columnType.dataType) //&& (foundColumn.nullable == columnType.nullable)
    }

    //end
    result
  }

  def isCompliantToSchema(dataFrame: DataFrame, structType: StructType,   checkUnkownColumn : Boolean = false): Boolean = {
    var result = true

    //check column
    structType.fields.foreach(column => {
      if (this.hasColumn(dataFrame, column) == false) result = false
    })

    //check size if enabled
    if(checkUnkownColumn && structType.fields.size != dataFrame.schema.fields.size){
      result = false
    }

    result
  }
}
