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

import eu.ill.puma.sparkmatcher.matching.pipepline._
import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import org.apache.spark.sql.SparkSession

class DataSourceStorage(sparkSession: SparkSession, val prod: Boolean = ProgramConfig.prod) {


  val testIdsSql = "IN (select distinct(proposal_id) from matching_static.test_match union select distinct(publication_id) from matching_static.test_match)"
  //  val testIdsSql = "IN ('46883','60874')"

  var personSql = "(select distinct case " +
    "    when l.lookup_id is null then p.id" +
    "    else l.person_id" +
    "  end as entity_id, a.document_version_id " +
    "  from person p, person_laboratory_affiliation a" +
    s"  left outer join ${ProgramConfig.startSchema}.person_deduplication_lookup l on l.lookup_id = a.person_id" +
    "  where p.id = a.person_id" +
    "  and p.obsolete = false" +
    "  and a.person_id is not null " +
    "  and a.document_version_id is not null  ##) person "

  if (prod) {
    personSql = personSql.replace("##", "")
  } else {
    personSql = personSql.replace("##", "and document_version_id " + testIdsSql)
  }


  var laboratorySql = "(select distinct case" +
    "    when l.lookup_id is null then p.id" +
    "    else l.laboratory_id" +
    "  end as entity_id, a.document_version_id " +
    "  from laboratory p, person_laboratory_affiliation a" +
    s"  left outer join ${ProgramConfig.startSchema}.laboratory_deduplication_lookup l on l.lookup_id = a.laboratory_id" +
    "  where p.id = a.laboratory_id" +
    "  and p.obsolete = false" +
    "  and a.laboratory_id is not null " +
    "  and a.document_version_id is not null  ## ) laboratory "

  if (prod) {
    laboratorySql = laboratorySql.replace("##", "")
  } else {
    laboratorySql = laboratorySql.replace("##", " and document_version_id " + testIdsSql)
  }


  var documentAddressSql = "(select distinct document_version_id, string_agg(address, ', ') as address " +
    "from laboratory, person_laboratory_affiliation " +
    "where laboratory.id = person_laboratory_affiliation.laboratory_id " +
    "and document_version_id is not null  ## " +
    "group by document_version_id ) address"

  if (prod) {
    documentAddressSql = documentAddressSql.replace("##", "")
  } else {
    documentAddressSql = documentAddressSql.replace("##", " and document_version_id " + testIdsSql)
  }


  var proposalCodeSql = "(select id as document_version_id, replace(short_name, '_refused', '') as entity from document_version where short_name is not null) query"

  var doiSql = "(select id as document_version_id, doi as entity from document_version where doi is not null) query"

  var pictureSql = s"( select document_version_id, id as entity_id, CONCAT('${ProgramConfig.fileRoot}', '/',file_path) as file_path from file " +
    s"where (mime_type = 'image/jpeg' or mime_type = 'image/jpg' or mime_type = 'image/png' or mime_type = 'image/gif')  ## ) files"

  if (prod) {
    pictureSql = pictureSql.replace("##", "")
  } else {
    pictureSql = pictureSql.replace("##", " and document_version_id " + testIdsSql)
  }

  val trainingIdSql = s"(select distinct(publication_id) as training_id, 'PUBLICATION' as training_type from matching_static.test_match union select distinct(proposal_id) as training_id, 'PROPOSAL' as training_type from matching_static.test_match) query"
//  val trainingIdSql = s"(select distinct document_version1_id as training_id, document_type    from matching_static.match_candidate_validation, puma.document, puma.document_version    where document_version.document_id = document.id    and document_version1_id = document_version.id  and accepted = true) query"

//  val trainingPairSql = s"(select proposal_id, publication_id from matching_static.test_match ) query"
//  val trainingPairSql = s"(select document_version1_id as proposal_id, document_version2_id as publication_id from matching_static.match_candidate_validation where accepted = true and document_version1_id < document_version2_id) query"

  val trainingPairSql ="(" +
    "SELECT DISTINCT CASE" +
    "           WHEN document_type='PUBLICATION' THEN document_version1_id " +
    "           ELSE document_version2_id " +
    "       END AS publication_id, " +
    "       CASE " +
    "           WHEN document_type='PROPOSAL' THEN document_version1_id " +
    "           ELSE document_version2_id " +
    "       END AS proposal_id " +
    "FROM matching_static.match_candidate_validation," +
    "     puma.document_version, " +
    "     puma.document " +
    "WHERE puma.document_version.document_id = document.id " +
    "  AND puma.document_version.document_id = document_version1_id " +
    "  AND accepted = TRUE\n  AND document_version1_id < document_version2_id " +
    "  AND document_type IN ('PUBLICATION', " +
    "                        'PROPOSAL')" +
    ") query"

  val typeSql = "(select document_version.id as document_version_id, document_type as document_type from document, document_version where document_version.document_id = document.id) type"

  val dateSql = "(select release_date as date, document_version.id as document_version_id, document_type from document, document_version where document_version.document_id = document.id) type"

  var fileSql = "(select document_version_id as document_version_id, file_path from file where mime_type = 'text/plain' and obsolete = false  ## ) file"

  if (prod) {
    fileSql = fileSql.replace("##", "")
  } else {
    fileSql = fileSql.replace("##", " and document_version_id " + testIdsSql)
  }

  var titleSql = "(select title as entity, id as document_version_id from document_version where title is not null  ## ) query"

  if (prod) {
    titleSql = titleSql.replace("##", "")
  } else {
    titleSql = titleSql.replace("##", " and  id " + testIdsSql)
  }

  var abstractSql = "(select abstract as text, id as document_version_id from document_version where abstract is not null  ## ) query"

  if (prod) {
    abstractSql = abstractSql.replace("##", "")
  } else {
    abstractSql = abstractSql.replace("##", " and  id " + testIdsSql)
  }

  val matchCandidateSql = "(select * from match_candidate) query"

  val pictureHashSql = "(select * from picture_hash) query"

  val matchCandidateStatsSql = s"(select * from ${ProgramConfig.startSchema}.match_candidate_stats) query"

  val validDocumentVersionIdSql = ("(select id as document_version_id from document_version where obsolete = false) query")

  val weightSql = ("(select * from matching.score_weight ) query")

  var instrumentSql = "(select code, name, instrument.id as entity_id, document_version_id from instrument, version_instrument where instrument.id = version_instrument.instrument_id  ## ) query"

  if (prod) {
    instrumentSql = instrumentSql.replace("##", "")
  } else {
    instrumentSql = instrumentSql.replace("##", " and document_version_id " + testIdsSql)
  }

  val instrumentNameSql =
    """(
    select
    instrument.id as instrument_id,
    laboratory.id as laboratory_id,
    instrument.name as instrument_name,
    instrument.code as instrument_code,
    instrument.analyser_confidence as instrument_confidence,
    laboratory.name as laboratory_name,
    laboratory.short_name as laboratory_short_name,
    laboratory.address as laboratory_address,
    laboratory.city as laboratory_city,
    laboratory.country as laboratory_country,
    array_agg(scientific_technique.name) as scientific_technique
    from instrument, laboratory, instrument_scientific_technique, scientific_technique
   where fixed = true
    and instrument.laboratory_id = laboratory.id
    and instrument.id = instrument_scientific_technique.instrument_id
    and instrument_scientific_technique.scientific_technique_id = scientific_technique.id
    group by
    instrument.id,
    laboratory.id,
    instrument.name,
    instrument.code,
    instrument.analyser_confidence,
    laboratory.name,
    laboratory.short_name,
    laboratory.address,
    laboratory.city,
    laboratory.country
) query """

  val instrumentAliasSql = "(select * from instrument_alias) query"

  val instrumentTechnicsSql = "(" +
    " select instrument_id, scientific_technique_id, document_version_id  " +
    s" from ${ProgramConfig.startSchema}.instrument, instrument_scientific_technique " +
    " where entity_id = instrument_id\nand (prediction = 1 or instrument_confidence = 100) " +
    " and instrument_confidence >= 0 ) query"

  var documentVersionSourceSql = "(select document_version_id, importer_short_name as entity from document_version_source) query"

  var keywordSql = "(select document_version_id, word as entity from keyword, version_keyword where keyword.id = version_keyword.keyword_id) query"

  var documentWordSpecSql = "(select document_version_id, lemma as entities  from document_word_spec) query"

  val trainingDataSql = "(select * from matching_static.training_data) query"

  val wordPosTagSql = "(select * from word_pos_tag) query"

  val techniqueSql = "(" +
    "select " +
    "  scientific_technique_id as tag_id, " +
    "  name, " +
    "  'technique' as tag, " +
    "  true as use_stemming " +
    "from " +
    "  scientific_technique_alias " +
    "union " +
    "select " +
    "  id as tag_id, " +
    "  name, " +
    "  'technique' as tag, " +
    "  true as use_stemming " +
    "from " +
    "  scientific_technique " +
    "where " +
    "  obsolete = 'false' " +
    ") query "

  val keywordDataSource = new SqlDataSource(sparkSession, keywordSql, EntitiesDfType, "keywordDataSource")
  val personDataSource = new SqlDataSource(sparkSession, personSql, EntitiesIdDfType, "personDataSource", Some("entity_id"))
  val laboratoryDataSource = new SqlDataSource(sparkSession, laboratorySql, EntitiesIdDfType, "laboratoryDataSource", Some("entity_id"))
  val documentAddressDataSource = new SqlDataSource(sparkSession, documentAddressSql, EntitiesDfType, "documentAddressDataSource", Some("document_version_id"))
  val typeDataSource = new SqlDataSource(sparkSession, typeSql, DocumentTypeDfType, "typeDataSource")
  val dateDateSource = new SqlDataSource(sparkSession, dateSql, DocumentDateDfType, "dateDateSource")
  val proposalCodeDataSource = new SqlDataSource(sparkSession, proposalCodeSql, EntitiesDfType, "proposalCodeDataSource")
  val doiDataSource = new SqlDataSource(sparkSession, doiSql, EntitiesDfType, "doiDataSource")
  val trainingIdsDataSource = new SqlDataSource(sparkSession, trainingIdSql, TrainingIdsDfType, "trainingIdsDataSource")
  val trainingPairDataSource = new SqlDataSource(sparkSession, trainingPairSql, TrainingPairDfType, "trainingIdsDataSource")
  val fileDataSource = new FileDataSource(sparkSession, fileSql, "fileDataSource")
  val advFileDataSource = new TextDataSource(fileDataSource, "advFileDataSource")
  val titleDataSource = new SqlDataSource(sparkSession, titleSql, EntitiesDfType, "titleDataSource", Some("document_version_id"))
  val abstractDataSource = new SqlDataSource(sparkSession, abstractSql, EntitiesDfType, "abstractDataSource", Some("document_version_id"))
  val textAbstractDataSource = new TextDataSource(abstractDataSource, "textAbstractDataSource")
  val matchCandidateDataSource = new SqlDataSource(sparkSession, matchCandidateSql, MatchCandidateDfType, "matchCandidateDataSource", Some("document_version1_id"))
  val pictureHashDataSource = new SqlDataSource(sparkSession, pictureHashSql, PictureHashDfType, "pictureHashDataSource", Some("document_version_id"))
  val matchCandidateStatsDataSource = new SqlDataSource(sparkSession, matchCandidateStatsSql, MatchCandidateStatsDfType, "matchCandidateStatsDataSource")
  val validDocumentVersionIdDataSource = new SqlDataSource(sparkSession, validDocumentVersionIdSql, DocumentVersionIdDfType, "validDocumentVersionIdDataSource")
  val weightDataSource = new SqlDataSource(sparkSession, weightSql, WeightEvaluatorDfType, "weightDataSource")
  val instrumentDataSource = new SqlDataSource(sparkSession, instrumentSql, EntitiesIdDfType, "instrumentDataSource")
  val instrumentNameDataSource = new SqlDataSource(sparkSession, instrumentNameSql, EntitiesIdDfType, "instrumentNameDataSource")
  val instrumentAliasDataSource = new SqlDataSource(sparkSession, instrumentAliasSql, EntitiesIdDfType, "instrumentAliasSql")
  val instrumentTechnicsDataSource = new SqlDataSource(sparkSession, instrumentTechnicsSql, InstrumentTechniqueDfType, "technicsDataSource")
  val documentVersionSourceDataSource = new SqlDataSource(sparkSession, documentVersionSourceSql, EntitiesDfType, "documentVersionSourceDataSource")
  val documentWordSpecDataSource = new SqlDataSource(sparkSession, documentWordSpecSql, EntitiesArrayDfType, "wordIdfDataSource")
  val trainingDataDataSource = new SqlDataSource(sparkSession, trainingDataSql, EntitiesArrayDfType, "trainingDataDataSource")
  val wordPosTagDataSource = new SqlDataSource(sparkSession, wordPosTagSql, EntitiesDfType, "wordPosTagDataSource")
  val techniqueDataSource = new SqlDataSource(sparkSession, techniqueSql, ScientificTechniqueDfType, "technicsDataSource")


  pictureHashDataSource.dataBaseUrl = ProgramConfig.matchingDatabaseUrl
  matchCandidateDataSource.dataBaseUrl = ProgramConfig.matchingDatabaseUrl
  documentWordSpecDataSource.dataBaseUrl = ProgramConfig.matchingDatabaseUrl
  trainingDataDataSource.dataBaseUrl = ProgramConfig.matchingDatabaseUrl
  wordPosTagDataSource.dataBaseUrl = ProgramConfig.matchingDatabaseUrl


  val pictureDataSource = new SqlDataSource(sparkSession, pictureSql, EntitiesIdDfType, "pictureDataSource")
}
