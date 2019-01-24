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

import eu.ill.puma.sparkmatcher.matching.datasource.DataSource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class PersonTechniqueFilter(personDataSource: DataSource, personProbabilityThreshold: Double = 0.5) extends Filter {


  override def run(input: DataFrame): DataFrame = {

    import input.sparkSession.implicits._

    /**
      * Input
      */
    val person = personDataSource.loadData._2.select($"document_version_id", $"entity_id" as "person_id")

    val potentialTechnique = input
      .select($"tag_id" as "technique_id", $"document_version_id", $"feature_ngram", $"feature_idf", $"feature_frequency")
      .distinct()

    /**
      * compute stats
      */

    //get technique by person and document
    val techniqueByPersonAndDocument = potentialTechnique
      .select($"technique_id", $"document_version_id")
      .join(person, Seq("document_version_id"))
      .cache()

    //get technique used by each person
    val techniqueByPerson = techniqueByPersonAndDocument
      .groupBy($"person_id", $"technique_id")
      .agg(count($"*") as "person_technique_count")

    //get the total number of technique used by the person
    val techniqueCountByPerson = techniqueByPersonAndDocument
      .groupBy($"person_id")
      .agg(count($"*") as "person_total_technique_count")

    //join pervious data to built the probabilistic model
    val model = techniqueByPerson.join(techniqueCountByPerson, Seq("person_id"))
      .withColumn("person_technique_probability", $"person_technique_count" / $"person_total_technique_count")
      .cache()


    //compute stats per document and technics
    val technics = model
      .join(techniqueByPersonAndDocument, Seq("technique_id", "person_id"))
      .groupBy($"document_version_id", $"technique_id")
      .agg(
        collect_list(struct($"person_id", $"person_technique_probability")) as "person",
        sum($"person_technique_probability") as "total_probabilty",
        avg($"person_technique_probability") as "avg_probabilty",
        count($"person_id") as "count"
      )

    val interval = 0.05


    val result = technics
      .filter($"count" > 2)
      .withColumn("range", $"avg_probabilty" - ($"avg_probabilty" % interval))
      .withColumn("range", concat($"range", lit(" - "), $"range" + interval)) //optional one
      .groupBy($"range")
      .count()


    //      .filter($"avg_probabilty" > this.personProbabilityThreshold)


    result.show(false)


    input
  }

  override def name: String = "instrument technique filter"
}
