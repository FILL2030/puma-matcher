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
package eu.ill.puma.sparkmatcher.deduplication.dedup

import java.sql.{Connection, DriverManager}

import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig

object DocumentDeduplicatorApp {

	def main(args: Array[String]) {
		DocumentDeduplicatorApp.run
	}
	def run = {

		var connection: Connection = null

		try {
			Class.forName(ProgramConfig.driver)
			connection = DriverManager.getConnection(ProgramConfig.corpusDatabaseUrl, ProgramConfig.dbProperties)

			val duplicatedDocumentsTable = """
				  |with duplicated_document as (
				  |select dv1.id as wos_dv_id, dv2.id as other_dv_id, s2.importer_short_name as other_importer, d1.id as wos_document_id, d2.id as other_document_id, lower(dv1.doi) as wos_doi, lower(dv2.doi) as other_doi, dv1.title as wos_title, dv2.title as other_title, dv1.obsolete as wos_dv_obsolete, dv2.obsolete as other_dv_obsolete
				  |  from document_version dv1, document_version dv2, document_version_source s1, document_version_source s2, document d1, document d2
				  |  where lower(dv1.doi) = lower(dv2.doi)
				  |  and dv1.doi is not null
				  |  and dv1.id != dv2.id
				  |  and s1.document_version_id = dv1.id
				  |  and s2.document_version_id = dv2.id
				  |  and dv1.document_id = d1.id
				  |  and dv2.document_id = d2.id
				  |  and s1.importer_short_name = ?
				  |  and s2.importer_short_name != ?
				  |union
				  |select dv1.id as wos_dv_id, dv2.id as other_dv_id, s2.importer_short_name as other_importer, d1.id as wos_document_id, d2.id as other_document_id, lower(dv1.doi) as wos_doi, lower(dv2.doi) as other_doi, dv1.title as wos_title, dv2.title as other_title, dv1.obsolete as wos_dv_obsolete, dv2.obsolete as other_dv_obsolete
				  |  from document_version dv1, document_version dv2, document_version_source s1, document_version_source s2, document d1, document d2
				  |  where lower(dv1.doi) = lower(dv2.doi)
				  |  and dv1.doi is not null
				  |  and dv1.id < dv2.id
				  |  and s1.document_version_id = dv1.id
				  |  and s2.document_version_id = dv2.id
				  |  and dv1.document_id = d1.id
				  |  and dv2.document_id = d2.id
				  |  and s1.importer_short_name = ?
				  |  and s2.importer_short_name = ?
				  |)
				""".stripMargin

			val makeDocumentVersionsObsolete = duplicatedDocumentsTable + """
				  |update document_version dv
				  |  set obsolete = true
				  |  where dv.id in (select other_dv_id from duplicated_document);
				""".stripMargin

			val makeDocumentVersionsObsoleteStatement = connection.prepareStatement(makeDocumentVersionsObsolete)
			makeDocumentVersionsObsoleteStatement.setString(1, "wos")
			makeDocumentVersionsObsoleteStatement.setString(2, "wos")
			makeDocumentVersionsObsoleteStatement.setString(3, "wos")
			makeDocumentVersionsObsoleteStatement.setString(4, "wos")
			var numberOfRowsUpdated = makeDocumentVersionsObsoleteStatement.executeUpdate()
			println("Made " + numberOfRowsUpdated + " document versions obsolete")

			val setOriginalDocumentId = duplicatedDocumentsTable + """
				  |update document_version dv
				  |  set original_document_id = document_id
				  |  where dv.id in (select other_dv_id from duplicated_document)
				  |  and dv.original_document_id is null;
				""".stripMargin

			val setOriginalDocumentIdStatement = connection.prepareStatement(setOriginalDocumentId)
			setOriginalDocumentIdStatement.setString(1, "wos")
			setOriginalDocumentIdStatement.setString(2, "wos")
			setOriginalDocumentIdStatement.setString(3, "wos")
			setOriginalDocumentIdStatement.setString(4, "wos")
			numberOfRowsUpdated = setOriginalDocumentIdStatement.executeUpdate()
			println("Set " + numberOfRowsUpdated + " original document version Id")

			val updateDocumentId = duplicatedDocumentsTable + """
				|update document_version dv
				|  set document_id = (select dd.wos_document_id
				|    from duplicated_document dd
				|    where dd.other_dv_id = dv.id
				|    and dd.wos_dv_obsolete = false)
				|  where dv.id in (select other_dv_id from duplicated_document);
			  """.stripMargin

			val updateDocumentIdStatement = connection.prepareStatement(updateDocumentId)
			updateDocumentIdStatement.setString(1, "wos")
			updateDocumentIdStatement.setString(2, "wos")
			updateDocumentIdStatement.setString(3, "wos")
			updateDocumentIdStatement.setString(4, "wos")
			numberOfRowsUpdated = updateDocumentIdStatement.executeUpdate()
			println("Deduplicated " + numberOfRowsUpdated + " document versions")

		} catch {
			case e: Throwable => e.printStackTrace()
		}

		connection.close()
	}
}