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
package eu.ill.puma.sparkmatcher.matching.filter

import eu.ill.puma.sparkmatcher.matching.pipepline.EntityType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class MultipleTypeMatchFilter(allowedSingleType: EntityType*) extends Filter {
  override def run(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._

    //build filter expression
    var filterExpr = size($"score_type") > 1

    //append each single type
    allowedSingleType.foreach(entityType => {
      filterExpr = filterExpr || array_contains($"score_type", entityType.stringValue)
    })

    //filter
    val validPairId = input.groupBy($"document_version1_id", $"document_version2_id").agg(
      collect_list("score_type") as "score_type"
    ).filter(filterExpr)
      .drop($"score_type")

    input.join(validPairId, Seq("document_version1_id", "document_version2_id"))
  }

  override def name: String = "match filter"
}
