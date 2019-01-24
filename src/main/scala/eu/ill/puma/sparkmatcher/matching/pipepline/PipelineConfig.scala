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

import eu.ill.puma.sparkmatcher.matching.analyser.Analyser
import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import eu.ill.puma.sparkmatcher.matching.matcheditor.MatchEditor
import eu.ill.puma.sparkmatcher.matching.matcher.Matcher
import eu.ill.puma.sparkmatcher.matching.scorer.Scorer

import scala.collection.mutable.ListBuffer


class PipelineConfig(val name: String) {

  private var _dataSource: Option[DataSource] = None
  private var _analyser: Option[Analyser] = None
  private var _matcher: Option[Matcher] = None
  private var _scorer: Option[Scorer] = None
  private var _matchEntityType: Option[EntityType] = None

  private var _matchEditors = ListBuffer.empty[MatchEditor]


  def dataSource = _dataSource

  def dataSource_=(value: DataSource): Unit = _dataSource = Some(value)


  def analyser = _analyser

  def analyser_=(value: Analyser): Unit = _analyser = Some(value)


  def matcher = _matcher

  def matcher_=(value: Matcher): Unit = _matcher = Some(value)


  def scorer = _scorer

  def scorer_=(value: Scorer): Unit = _scorer = Some(value)


  def matchEntityType = _matchEntityType

  def matchEntityType_=(value: EntityType): Unit = _matchEntityType = Some(value)


  def matchEditors = _matchEditors

  def addMatchEditor(matchAnalyser: MatchEditor) = _matchEditors.append(matchAnalyser)
}
