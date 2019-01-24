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

import eu.ill.puma.sparkmatcher.matching.datasource.SchemaStorage
import eu.ill.puma.sparkmatcher.matching.stage.WeightTrainerStage
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

sealed abstract class DataFrameType(val stringValue: String, val schema: StructType = SchemaStorage.emptySchema) extends Serializable {
  def encoder: ExpressionEncoder[Row] = {
    RowEncoder.apply(schema)
  }
}

case object EntitiesDfType extends DataFrameType("EntitiesDfType", SchemaStorage.entitiesSchema)

case object EntitiesArrayDfType extends DataFrameType("EntitiesDfType", SchemaStorage.entitiesArraySchema)

case object EntitiesIdDfType extends DataFrameType("EntitiesIdDfType", SchemaStorage.entitiesIdSchema)

case object PictureHashDfType extends DataFrameType("PictureHashDfType", SchemaStorage.pictureHashSchema)

case object TextDfType extends DataFrameType("TextDfType", SchemaStorage.textSchema)

case object AdvTextDfType extends DataFrameType("AdvTextDfType", SchemaStorage.advTextSchema)

case object FilteredSimilarityDfType extends DataFrameType("FilteredSimilarityDfType")

case object MatchCandidateDfType extends DataFrameType("MatchCandidateDfType", SchemaStorage.matchCandidateSchema)

case object MatchCandidateWithoutTypeDfType extends DataFrameType("MatchCandidateDfType", SchemaStorage.matchCandidateSchemaWithoutType)

case object MatchCandidateStatsDfType extends DataFrameType("MatchCandidateStatsDfType", SchemaStorage.matchCandidateStatsSchema)

case object DocumentTypeDfType extends DataFrameType("DocumentTypeDfType", SchemaStorage.documentTypeSchema)

case object DocumentVersionIdDfType extends DataFrameType("DocumentVersionIdDfType", SchemaStorage.documentVersionIdSchema)

case object DocumentDateDfType extends DataFrameType("DocumentDateDfType", SchemaStorage.documentDateSchema)

case object TrainingIdsDfType extends DataFrameType("TrainingIdsDfType", SchemaStorage.trainingIdSchema)

case object TrainingPairDfType extends DataFrameType("TrainingIdsDfType", SchemaStorage.trainingPairSchema)

case object TrainingDfType extends DataFrameType("TrainingDfType", SchemaStorage.trainingDocumentSchema)

case object WeightEvaluatorDfType extends DataFrameType("TrainingDfType", WeightTrainerStage.weightEvaluatorOutputSchema)

case object DocumentWordSpecDfType extends DataFrameType("DocumentWordSpecDfType", SchemaStorage.documentWordSpecSchema)

case object InstrumentTechniqueDfType extends DataFrameType("InstrumentTechniqueDfType", SchemaStorage.instrumentTechniqueSchema)

case object ScientificTechniqueDfType extends DataFrameType("ScientificTechniqueDfType", SchemaStorage.scientificTechnicSchema)

case object TextEntitiesAnalyserDfType extends DataFrameType("TextEntitiesAnalyserDfType", SchemaStorage.textEntitiesAnalyserSChema)


sealed abstract class SimilarityDataFrameType(val stringValues: String) extends DataFrameType(stringValues)

case object EntitySimilarityDfType extends SimilarityDataFrameType("EntitySimilarityDfType")

case object CosineSimilarityDfType extends SimilarityDataFrameType("CosineSimilarityDfType")

case object TextSimilarityDfType extends SimilarityDataFrameType("TextSimilarityDfType")

case object PictureSimilarityDfType extends SimilarityDataFrameType("PictureSimilarityDfType")

