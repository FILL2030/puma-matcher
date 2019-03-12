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

sealed abstract class StageName(val stringValue: String) extends Serializable

case object InitialisationStageName extends StageName("init stage")

case object AnalysisStageName extends StageName("analysis stage")

case object MatchingStageName extends StageName("match stage")

case object FilterStageName extends StageName("filter stage")

case object ScoringStageName extends StageName("score stage")

case object CacheStageName extends StageName("cache stage")

case object NormalisationStageName extends StageName("normalisation stage")

case object EditorStageName extends StageName("editor stage")

case object MatchCandidatePersisterStageName extends StageName("match candidate persister stage")

case object GenericPersisterStageName extends StageName("generic persister stage")

case object StatisticStageName extends StageName("statistic stage")

case object WeightEvaluatorStageName extends StageName("weight evaluator stage")

case object TrainingStageName extends StageName("training stage")

case object ViewStageName extends StageName("view stage")

case object CountStageName extends StageName("count stage")

case object ScoreListingStageName extends StageName("score list stage")

case object TotalScoreStageName extends StageName("total score stage")
