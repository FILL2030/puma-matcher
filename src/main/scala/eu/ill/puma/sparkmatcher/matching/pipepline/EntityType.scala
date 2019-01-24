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

import scala.collection.mutable.ListBuffer

sealed abstract class EntityType(val stringValue: String, val id: Int) extends Serializable

case object NoType extends EntityType("null", 0)

case object PersonType extends EntityType("person", 1)

case object FormulaType extends EntityType("formula", 2)

case object TextType extends EntityType("text", 3)

case object AbstractType extends EntityType("abstract", 4)

case object TitleType extends EntityType("title", 5)

case object CosineTitleType extends EntityType("cosine_title", 15)

case object DoiType extends EntityType("doi", 6)

case object ReferencedDoiType extends EntityType("referenced_doi", 7)

case object ProposalCodeType extends EntityType("proposal_code", 8)

case object ReferencedProposalCodeType extends EntityType("referenced_proposal_code", 9)

case object LaboratoryType extends EntityType("laboratory", 10)

case object RareWordType extends EntityType("rareword", 11)

case object PictureType extends EntityType("picture", 12)

case object TotalType extends EntityType("total", 13)

case object MatcherNumberType extends EntityType("matcher_number", 16)

case object InstrumentType extends EntityType("instrument", 17)

case object ScientificTechniqueType extends EntityType("scientifique_technique", 18)


object EntityType {
  var types = ListBuffer.empty[EntityType]
  this.register(NoType)
  this.register(PersonType)
  this.register(FormulaType)
  this.register(TextType)
  this.register(AbstractType)
  this.register(TitleType)
  this.register(DoiType)
  this.register(ReferencedDoiType)
  this.register(ProposalCodeType)
  this.register(ReferencedProposalCodeType)
  this.register(LaboratoryType)
  this.register(RareWordType)
  this.register(PictureType)
  this.register(CosineTitleType)
  this.register(TotalType)
  this.register(MatcherNumberType)
  this.register(InstrumentType)
  this.register(ScientificTechniqueType)

  def register(entityType: EntityType) = {
    types.append(entityType)
  }

  def getByValue(value: String): EntityType = {
    types.filter(_.stringValue == value).head
  }

  def getById(id: Int): EntityType = {
    types.filter(_.id == id).head
  }
}