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

import org.apache.spark.sql.types._

object SchemaStorage {
  val emptySchema = StructType(Array.empty[StructField])

  val entitiesSchema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false),
    StructField("entity", StringType, nullable = false)
  ))

  val entitiesArraySchema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false),
    StructField("entities", ArrayType(StringType), nullable = false)
  ))


  val entitiesIdSchema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false),
    StructField("entity_id", LongType, nullable = false)
  ))

  val documentTypeSchema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false),
    StructField("document_type", StringType, nullable = false)
  ))

  val documentDateSchema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false),
    StructField("document_type", StringType, nullable = false),
    StructField("date", DateType, nullable = false)
  ))

  val trainingIdSchema = StructType(Array(
    StructField("training_id", LongType, nullable = false),
    StructField("training_type", StringType, nullable = false)
  ))

  val trainingPairSchema = StructType(Array(
    StructField("proposal_id", LongType, nullable = false),
    StructField("publication_id", LongType, nullable = false)
  ))

  val matchCandidateSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("document_version1_id", LongType, nullable = false),
    StructField("document_version2_id", LongType, nullable = false),
    StructField("document1_type", StringType, nullable = false),
    StructField("document2_type", StringType, nullable = false),
    StructField("score_type", StringType, nullable = false),
    StructField("score", DoubleType, nullable = false),
    StructField("item_count", LongType, nullable = false),
    StructField("pair_id", LongType, nullable = false)
  ))

  val matchCandidateSchemaWithoutType = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("document_version1_id", LongType, nullable = false),
    StructField("document_version2_id", LongType, nullable = false),
    StructField("score_type", StringType, nullable = false),
    StructField("score", DoubleType, nullable = false),
    StructField("item_count", LongType, nullable = false)
  ))

  val matchCandidateStatsSchema = StructType(
    Array(
      StructField("document_version_id", LongType, nullable = false),
      StructField("score_type", StringType, nullable = false),
      StructField("matched_document_version_ids", ArrayType(LongType), nullable = false),
      StructField("match_ids", ArrayType(LongType), nullable = false),
      StructField("scores", ArrayType(DoubleType), nullable = false),
      StructField("rank", ArrayType(LongType), nullable = false),
      StructField("min_score", DoubleType, nullable = false),
      StructField("max_score", DoubleType, nullable = false),
      StructField("match_count", IntegerType, nullable = false),
      StructField("average_score", DoubleType, nullable = false),
      StructField("std", DoubleType, nullable = false),
      StructField("median", DoubleType, nullable = false),
      StructField("percentile10_count", IntegerType, nullable = false),
      StructField("percentile20_count", IntegerType, nullable = false),
      StructField("percentile50_count", IntegerType, nullable = false),
      StructField("percentile80_count", IntegerType, nullable = false),
      StructField("percentile90_count", IntegerType, nullable = false),
      StructField("percentile10", DoubleType, nullable = false),
      StructField("percentile20", DoubleType, nullable = false),
      StructField("percentile50", DoubleType, nullable = false),
      StructField("percentile80", DoubleType, nullable = false),
      StructField("percentile90", DoubleType, nullable = false),
      StructField("percentile10_percent", DoubleType, nullable = false),
      StructField("percentile20_percent", DoubleType, nullable = false),
      StructField("percentile50_percent", DoubleType, nullable = false),
      StructField("percentile80_percent", DoubleType, nullable = false),
      StructField("percentile90_percent", DoubleType, nullable = false)
    )
  )

  val advTextSchema = StructType(
    Array(
      StructField("document_version_id", LongType, nullable = false),
      StructField("feature", ArrayType(StringType), nullable = false),
      StructField("position", ArrayType(LongType), nullable = false),
      StructField("words", ArrayType(StringType), nullable = false)
    )
  )

  val textSchema = StructType(
    Array(
      StructField("document_version_id", LongType, nullable = false),
      StructField("text", StringType, nullable = false)
    )
  )

  val pictureHashSchema = StructType(
    Array(
      StructField("document_version_id", LongType, nullable = false),
      StructField("entity_id", LongType, nullable = false),
      StructField("file_path", StringType, nullable = false),
      StructField("width", LongType, nullable = false),
      StructField("height", LongType, nullable = false),
      StructField("hash", StringType, nullable = false)
    )
  )

  val documentVersionIdSchema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false)
  ))

  val trainingDocumentSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("match_id", LongType, nullable = false),
    StructField("training_id", LongType, nullable = false),
    StructField("other_document_version_id", LongType, nullable = false),
    StructField("training_type", StringType, nullable = false),
    StructField("score_type", StringType, nullable = false),
    StructField("score", DoubleType, nullable = false),
    StructField("item_count", LongType, nullable = false)
  ))

  val weightEvaluatorSchema = StructType(Array(
    StructField("weight_id", LongType, nullable = false),
    StructField("document_version_id", LongType, nullable = false),
    StructField("score_type", StringType, nullable = false),
    StructField("matched_document_version_ids", ArrayType(LongType), nullable = false),
    StructField("match_ids", ArrayType(LongType), nullable = false),
    StructField("scores", ArrayType(DoubleType), nullable = false),
    StructField("rank", ArrayType(LongType), nullable = false),
    StructField("min_score", DoubleType, nullable = false),
    StructField("max_score", DoubleType, nullable = false),
    StructField("match_count", IntegerType, nullable = false),
    StructField("average_score", DoubleType, nullable = false),
    StructField("std", DoubleType, nullable = false),
    StructField("median", DoubleType, nullable = false)
  ))

  val documentWordSpecSchema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false),
    StructField("total_distance", DoubleType, nullable = true),
    StructField("avg_distance", DoubleType, nullable = true),
    StructField("count", LongType, nullable = false),
    StructField("lemma", ArrayType(StringType), nullable = true)
  ))

  val scientificTechnicSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("tag", StringType, nullable = false),
    StructField("use_stemming", BooleanType, nullable = false)
  ))

  val instrumentTechniqueSchema = StructType(Array(
    StructField("instrument_id", LongType, nullable = false),
    StructField("scientific_technique_id", LongType, nullable = false),
    StructField("instrumentTechnicsSql", LongType, nullable = false)
  ))

  val textEntitiesAnalyserSChema = StructType(Array(
    StructField("document_version_id", LongType, nullable = false),
    StructField("feature_position", IntegerType, nullable = true),
    StructField("feature_stem", StringType, nullable = true),
    StructField("feature_ngram", StringType, nullable = false),
    StructField("feature_size", IntegerType, nullable = false),
    StructField("feature_idf", DoubleType, nullable = false),
    StructField("feature_frequency", LongType, nullable = false),
    StructField("tag", StringType, nullable = true),
    StructField("tag_id", LongType, nullable = true),
    StructField("ngram_index", LongType, nullable = false),
    StructField("sentence", ArrayType(StringType), nullable = true)
  ))
}
