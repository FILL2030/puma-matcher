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
package eu.ill.puma.sparkmatcher.utils.database

import java.sql.{Connection, DriverManager}

import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig
import eu.ill.puma.sparkmatcher.utils.logger.Logger

object DbManager {

  def fullReset: Unit = {
    Logger.info("DbManager", "no config", "Reset DataBase")
    DbManager.resetMatchCandidate()
    DbManager.resetTrainingMatchCandidate()
    DbManager.resetMatchCandidateStats()
    DbManager.resetEntitiesSimilarities()
    DbManager.resetWordSimilarities()
    DbManager.resetMatchInfo()
    DbManager.resetFormula()
    DbManager.resetProposalCode()
    DbManager.resetDoi()
    DbManager.resetExcludedEntities()
    DbManager.resetPictureHash()
    DbManager.resetPictureSimilarities()
    DbManager.resetCosineSimilarities()
    DbManager.resetWeight()
    DbManager.resetTotalMatchCandidateStats()
    DbManager.resetInstrument()
    DbManager.resetWordSpec()
    //    DbManager.resetPersonDeduplicationLookup()
    //    DbManager.resetLaboratoryDeduplicationLookup()
  }

  def resetMatchCandidate(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS match_candidate;"

    val matchCandidateCreationQuery = "CREATE TABLE match_candidate (" +
      " id bigint NOT NULL," +
      " document_version1_id bigint NOT NULL," +
      " document_version2_id bigint NOT NULL," +
      " document1_type varchar(50) NOT NULL," +
      " document2_type varchar(50) NOT NULL," +
      " score_type varchar(50) NOT NULL," +
      " score double precision NOT NULL," +
      " item_count bigint NOT NULL," +
      " pair_id bigint NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX match_candidate_idx ON match_candidate (document_version1_id, document_version2_id, score_type, pair_id);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetTrainingMatchCandidate(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS training_match_candidate;"

    val matchCandidateCreationQuery = "CREATE TABLE training_match_candidate (" +
      " id bigserial NOT NULL," +
      " match_id bigint NOT NULL," +
      " training_id bigint NOT NULL," +
      " other_document_version_id bigint NOT NULL," +
      " training_type varchar(50) NOT NULL," +
      " score_type varchar(50) NOT NULL," +
      " score double precision NOT NULL," +
      " item_count bigint NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX training_match_candidate_idx ON training_match_candidate (training_id, other_document_version_id, score_type);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }


  def resetMatchCandidateStats(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS  match_candidate_stats;"

    val matchCandidateCreationQuery = "CREATE TABLE match_candidate_stats (" +
      " id bigserial NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " document_type varchar(50) NOT NULL," +
      " score_type varchar(50) NOT NULL," +
      " matched_document_version_ids bigint[]," +
      " match_ids bigint[]," +
      " scores double precision[], " +
      " rank bigint[]," +
      " min_score double precision NOT NULL," +
      " max_score double precision NOT NULL," +
      " match_count integer NOT NULL," +
      " average_score double precision NOT NULL," +
      " std double precision NOT NULL," +
      " median double precision NOT NULL," +
      " percentile10_count integer NOT NULL," +
      " percentile20_count integer NOT NULL," +
      " percentile50_count integer NOT NULL," +
      " percentile80_count integer NOT NULL," +
      " percentile90_count integer NOT NULL," +
      " percentile10 double precision NOT NULL," +
      " percentile20 double precision NOT NULL," +
      " percentile50 double precision NOT NULL," +
      " percentile80 double precision NOT NULL," +
      " percentile90 double precision NOT NULL," +
      " percentile10_percent double precision NOT NULL," +
      " percentile20_percent double precision NOT NULL," +
      " percentile50_percent double precision NOT NULL," +
      " percentile80_percent double precision NOT NULL," +
      " percentile90_percent double precision NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX match_candidate_stats_idx ON match_candidate_stats (document_version_id, document_type, score_type);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetTotalMatchCandidateStats(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS  total_match_candidate_stats;"

    val matchCandidateCreationQuery = "CREATE TABLE total_match_candidate_stats (" +
      " id bigserial NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " document_type varchar(50) NOT NULL," +
      " score_type varchar(50) NOT NULL," +
      " matched_document_version_ids bigint[]," +
      " match_ids bigint[]," +
      " total_scores double precision[], " +
      " total_contextual_scores double precision[], " +
      " rank bigint[]," +
      " min_score double precision NOT NULL," +
      " max_score double precision NOT NULL," +
      " min_contextual_score double precision NOT NULL," +
      " max_contextual_score double precision NOT NULL," +
      " match_count integer NOT NULL," +
      " average_score double precision NOT NULL," +
      " std double precision NOT NULL," +
      " median double precision NOT NULL," +
      " percentile10_count integer NOT NULL," +
      " percentile20_count integer NOT NULL," +
      " percentile50_count integer NOT NULL," +
      " percentile80_count integer NOT NULL," +
      " percentile90_count integer NOT NULL," +
      " percentile10 double precision NOT NULL," +
      " percentile20 double precision NOT NULL," +
      " percentile50 double precision NOT NULL," +
      " percentile80 double precision NOT NULL," +
      " percentile90 double precision NOT NULL," +
      " percentile10_percent double precision NOT NULL," +
      " percentile20_percent double precision NOT NULL," +
      " percentile50_percent double precision NOT NULL," +
      " percentile80_percent double precision NOT NULL," +
      " percentile90_percent double precision NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX total_match_candidate_stats_idx ON total_match_candidate_stats (document_version_id, document_type, score_type);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetEntitiesSimilarities(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS entities_similarities;"

    val matchCandidateCreationQuery = "CREATE TABLE entities_similarities (" +
      " id bigserial NOT NULL," +
      " document_version1_id bigint NOT NULL," +
      " document_version2_id bigint NOT NULL," +
      " match_id bigint NOT NULL," +
      " type varchar(50) NOT NULL," +
      " entities_count bigint," +
      " entities_ids bigint[]," +
      " entities_score double precision[]," +
      " score_factor double precision," +
      " score double precision," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX entities_similarities_idx ON entities_similarities (document_version1_id, document_version2_id, match_id, type);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }


  def resetWordSimilarities(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS word_similarities;"

    val matchCandidateCreationQuery = "CREATE TABLE word_similarities (" +
      " id bigserial NOT NULL," +
      " document_version1_id bigint NOT NULL," +
      " document_version2_id bigint NOT NULL," +
      " match_id bigint NOT NULL," +
      " type varchar(50) NOT NULL," +
      " document_version1_position integer[] NOT NULL," +
      " document_version2_position integer[] NOT NULL," +
      " word text[] NOT NULL," +
      " score double precision NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX word_similarities_idx ON word_similarities (document_version1_id, document_version2_id, match_id, type);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }


  def resetInstrument(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS instrument;"

    val matchCandidateCreationQuery = "CREATE TABLE instrument (" +
      " id bigserial NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " entity_id bigint NOT NULL," +
      " row_id text NOT NULL," +
      " instrument_name text[] NOT NULL," +
      " duplicated_code boolean NOT NULL," +
      " instrument_sentence text," +
      " instrument_confidence integer," +
      " max_score bigint," +
      " prediction double precision," +
      " document_type varchar(50) NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX instrument_idx ON instrument (document_version_id, entity_id, instrument_name, duplicated_code, prediction);"


    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetTechnique(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS technique;"

    val matchCandidateCreationQuery = "CREATE TABLE technique (" +
      " id bigserial NOT NULL," +
      " row_id text NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " technique_id bigint NOT NULL," +
      " feature_position bigint NOT NULL," +
      " prediction double precision," +
      " score double precision," +
      " technique_name text[] NOT NULL," +
      " sentence text NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX technique_idx ON technique (document_version_id, technique_id, technique_name, prediction);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetFormula(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS formula;"

    val matchCandidateCreationQuery = "CREATE TABLE formula (" +
      " id bigserial NOT NULL," +
      " entity_id bigint NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " formula_code varchar(100) NOT NULL," +
      " tf bigint NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX formula_idx ON formula (document_version_id, entity_id);"


    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetProposalCode(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS proposal_code;"

    val matchCandidateCreationQuery = "CREATE TABLE proposal_code (" +
      " id bigserial NOT NULL," +
      " entity_id bigint NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " proposal_code varchar(100) NOT NULL," +
      " type varchar(100) NOT NULL," +
      " tf bigint NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX proposal_code_idx ON proposal_code (document_version_id, entity_id);"


    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetExcludedEntities(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS excluded_entities;"
    //
    //    val matchCandidateCreationQuery = "CREATE TABLE excluded_entities (" +
    //      " id bigserial NOT NULL," +
    //      " entity_id bigint NOT NULL," +
    //      " type varchar(100) NOT NULL" +
    //      ");"
    //
    DbManager.runQuery(matchCandidateDropQuery)
    //
    //    DbManager.runQuery(matchCandidateCreationQuery)
  }

  def resetDoi(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS doi;"

    val matchCandidateCreationQuery = "CREATE TABLE doi (" +
      " id bigserial NOT NULL," +
      " entity_id bigint NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " doi varchar(100) NOT NULL," +
      " type varchar(100) NOT NULL," +
      " tf bigint NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX doi_idx ON doi (document_version_id, entity_id);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetWordSpec(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS document_word_spec;"

    val matchCandidateCreationQuery = "CREATE TABLE document_word_spec (" +
      "    document_version_id bigint, " +
      "    word text, " +
      "    lemma text, " +
      "    pos_tag text, " +
      "    pcc_idf double precision, " +
      "    pcc_frequency bigint NOT NULL, " +
      "    english_idf double precision, " +
      "    english_frequency bigint," +
      "    distance double precision, " +
      "    sentence_position bigint, " +
      "    word_position bigint, " +
      "    sentence text[] " +
      ");"

    val indexQuery = "CREATE INDEX document_word_spec_idx ON document_word_spec (document_version_id, word);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }


  def resetMatchInfo(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS match_run_info;"

    val matchCandidateCreationQuery = "CREATE TABLE match_run_info (" +
      " id bigserial NOT NULL," +
      " key varchar(100) NOT NULL," +
      " value varchar(100) NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)
  }


  def resetPictureHash(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS picture_hash;"

    val matchCandidateCreationQuery = "CREATE TABLE IF NOT EXISTS picture_hash (" +
      " id bigserial NOT NULL," +
      " document_version_id bigint NOT NULL," +
      " entity_id bigint NOT NULL," +
      " file_path varchar(100) NOT NULL," +
      " width integer," +
      " height integer," +
      " hash varchar(256)," +
      " PRIMARY KEY (id)" +
      ");"

    //    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)
  }


  def resetPictureSimilarities(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS picture_similarities;"

    val matchCandidateCreationQuery = "CREATE TABLE picture_similarities (" +
      " id bigserial NOT NULL," +
      " match_id bigint NOT NULL," +
      " document_version1_id bigint NOT NULL," +
      " document_version2_id bigint NOT NULL," +
      " doc1_entity_id bigint NOT NULL," +
      " doc2_entity_id bigint NOT NULL," +
      " score_type varchar(100) NOT NULL," +
      " distance integer NOT NULL," +
      " approx double precision NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)
  }

  def resetCosineSimilarities(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS cosine_similarities;"

    val matchCandidateCreationQuery = "CREATE TABLE cosine_similarities (" +
      " id bigserial NOT NULL," +
      " document_version1_id bigint NOT NULL," +
      " document_version2_id bigint NOT NULL," +
      " match_id bigint NOT NULL," +
      " type varchar(50) NOT NULL," +
      " document_version1_feature text[] NOT NULL," +
      " document_version2_feature text[] NOT NULL," +
      " document_version1_position integer[] NOT NULL," +
      " document_version2_position integer[] NOT NULL," +
      " score double precision NOT NULL," +
      " common_feature text[] NOT NULL," +
      " PRIMARY KEY (id)" +
      ");"

    val indexQuery = "CREATE INDEX cosine_similarities_idx ON cosine_similarities (document_version1_id, document_version2_id, match_id, type);"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)

    DbManager.runQuery(indexQuery)
  }

  def resetWordPosTag(): Unit = {
    val dropQuery = "DROP TABLE IF EXISTS word_pos_tag;"

    val creationQuery = "CREATE TABLE IF NOT EXISTS word_pos_tag (" +
      " ngram text NOT NULL," +
      " pos_tag text NOT NULL" +
      ");"

    //    DbManager.runQuery(dropQuery)

    DbManager.runQuery(creationQuery)

  }

  def resetPersonDeduplicationLookup(): Unit = {
    val tableDropQuery = "DROP TABLE IF EXISTS person_deduplication_lookup;"

    val tableCreationQuery = "CREATE TABLE person_deduplication_lookup (" +
      " id bigserial primary key," +
      " lookup_id bigint NOT NULL," +
      " person_id bigint NOT NULL" +
      ");"

    val index1Query = "CREATE INDEX person_deduplication_lookup_lookup_id_index ON person_deduplication_lookup (lookup_id);"
    val index2Query = "CREATE INDEX person_deduplication_lookup_person_id_index ON person_deduplication_lookup (person_id);"

    DbManager.runQuery(tableDropQuery)

    DbManager.runQuery(tableCreationQuery)

    DbManager.runQuery(index1Query)
    DbManager.runQuery(index2Query)
  }

  def resetLaboratoryDeduplicationLookup(): Unit = {
    val tableDropQuery = "DROP TABLE IF EXISTS laboratory_deduplication_lookup;"

    val tableCreationQuery = "CREATE TABLE laboratory_deduplication_lookup (" +
      " id bigserial primary key," +
      " lookup_id bigint NOT NULL," +
      " laboratory_id bigint NOT NULL" +
      ");"

    val index1Query = "CREATE INDEX laboratory_deduplication_lookup_lookup_id_index ON laboratory_deduplication_lookup (lookup_id);"
    val index2Query = "CREATE INDEX laboratory_deduplication_lookup_laboratory_id_index ON laboratory_deduplication_lookup (laboratory_id);"

    DbManager.runQuery(tableDropQuery)

    DbManager.runQuery(tableCreationQuery)

    DbManager.runQuery(index1Query)
    DbManager.runQuery(index2Query)
  }

  def saveMatchInfo(key: String, value: String): Unit = {
    val query = "INSERT INTO match_run_info (key, value) VALUES ('" + key + "', '" + value + "');";
    this.runQuery(query)
  }

  def saveWeight(scoreType: String, value: Double): Unit = {
    val query = "INSERT INTO score_weight (score_type, weight) VALUES ('" + scoreType + "', '" + value + "');";
    this.runQuery(query)
  }

  def resetWeight(): Unit = {
    val matchCandidateDropQuery = "DROP TABLE IF EXISTS score_weight;"

    val matchCandidateCreationQuery = "CREATE TABLE score_weight (" +
      " score_type varchar(100) NOT NULL," +
      " weight double precision NOT NULL" +
      ");"

    DbManager.runQuery(matchCandidateDropQuery)

    DbManager.runQuery(matchCandidateCreationQuery)
  }

  def runQuery(query: String): Unit = {
    var connection: Connection = null

    try {
      Class.forName(ProgramConfig.driver)
      connection = DriverManager.getConnection(ProgramConfig.matchingDatabaseUrl, ProgramConfig.dbProperties)

      // create the statement, and run the select query
      val statement = connection.prepareStatement(query)
      statement.execute()

    } catch {
      case e: Throwable => e.printStackTrace()
    }
    connection.close()
  }

  def runIntQuery(query: String): Int = {
    var connection: Connection = null
    var result = 0

    try {
      Class.forName(ProgramConfig.driver)
      connection = DriverManager.getConnection(ProgramConfig.corpusDatabaseUrl, ProgramConfig.dbProperties)

      // create the statement, and run the select query
      val statement = connection.prepareStatement(query)
      val resultSet = statement.executeQuery()

      if (resultSet.next()) {
        result = resultSet.getInt(1)
      }

    } catch {
      case e: Throwable => e.printStackTrace()
        println(query)
        System.exit(1)
    }
    connection.close()

    result
  }

  def runIntQuery(query: String, dataBaseUrl: String): Int = {
    var connection: Connection = null
    var result = 0

    try {
      Class.forName(ProgramConfig.driver)
      connection = DriverManager.getConnection(dataBaseUrl, ProgramConfig.dbProperties)

      // create the statement, and run the select query
      val statement = connection.prepareStatement(query)
      val resultSet = statement.executeQuery()

      if (resultSet.next()) {
        result = resultSet.getInt(1)
      }

    } catch {
      case e: Throwable => e.printStackTrace()
        println(query)
        System.exit(1)
    }
    connection.close()

    result
  }

  def swapSchema: Unit = {
    Logger.info("DbManager", "no config", "Swapping schema")


    val schemaQuery1 = "  ALTER SCHEMA " + ProgramConfig.startSchema + " RENAME TO tmp_schema ";
    DbManager.runQuery(schemaQuery1)

    val schemaQuery2 = "  ALTER SCHEMA " + ProgramConfig.finalSchema + " RENAME TO " + ProgramConfig.startSchema + " ;"
    DbManager.runQuery(schemaQuery2)

    val schemaQuery3 = "  ALTER SCHEMA tmp_schema RENAME TO " + ProgramConfig.finalSchema + " ;"
    DbManager.runQuery(schemaQuery3)
  }

}
