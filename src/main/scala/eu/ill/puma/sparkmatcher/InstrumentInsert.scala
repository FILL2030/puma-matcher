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
package eu.ill.puma.sparkmatcher

import eu.ill.puma.sparkmatcher.utils.conf.ProgramConfig.getClass

import scala.io.Source

object InstrumentInsert {

  def main(args: Array[String]): Unit = {

    val laboratoryShortnameIndex = 0
    val laboratoryNameIndex = 1
    val laboratoryCountryIndex = 2
    val laboratoryCityIndex = 3
    val laboratoryAddressIndex = 4

    val instrumentCodeIndex = 5
    val instrumentNameIndex = 6
    val instrumentTechniqueIndex = 7


    //read csv file
    val data = Source.fromURL(getClass.getResource("/instruments.csv"))
      .getLines()
      .toList
      .drop(1)
      .map(line => {

        //remove first line and convert to list
        val elems = line.substring(1).split("#").toList

        println("-- parse line" + line)

        //build intrumen tuple
        val instrument =
          (
            (
              elems(laboratoryShortnameIndex),
              elems(laboratoryNameIndex),
              elems(laboratoryCountryIndex),
              elems(laboratoryCityIndex),
              elems(laboratoryAddressIndex)
            ),
            (
              elems(instrumentCodeIndex),
              elems(instrumentNameIndex),
              elems(instrumentTechniqueIndex)
            )
          )

        instrument
      })


    //extract technique
    var techniqueMap = data.flatMap(_._2._3.split("&")).sorted.toSet.zipWithIndex.map(x => (x._1, x._2 + 1)).toMap

    //technique insert
    techniqueMap foreach { case (name, id) => {
      println(s"INSERT INTO scientific_technique (name) VALUES ('${name.toLowerCase}');")
    }
    }


    //group by labo
    data.groupBy(_._1).foreach(lab => {

      //labo insert sql
      println()
      println(s"INSERT INTO laboratory (short_name, name, country, city, address) VALUES ('${lab._1._1.toLowerCase}', '${lab._1._2.toLowerCase}', '${lab._1._3.toLowerCase}', '${lab._1._4.toLowerCase}', '${lab._1._5.toLowerCase}');")

      //for each instrument
      lab._2.foreach(instr => {

        //extract instrument data
        val rawInstrumentCodes = instr._2._1
        val instrumentName = instr._2._2.replaceAll("\"", "")
        val rawTechniques = instr._2._3
        val instrumentCodes = rawInstrumentCodes.split("&").toSet
        val instrumentCode = instrumentCodes.head

        //instrument insert sql
        println(s"INSERT INTO instrument (code, name,  laboratory_id, fixed) VALUES ('${instrumentCode.toLowerCase}','${instrumentName.toLowerCase}', currval('laboratory_id_seq'::regclass), true);")

        //instrument alias sql
        if (instrumentCodes.size > 1) instrumentCodes.drop(1).foreach(alias => {
          println(s"INSERT INTO instrument_alias (instrument_id, alias) VALUES (currval('instrument_id_seq'::regclass), '${alias.toLowerCase}');")
        })

        //technique insert sql
        rawTechniques.split("&").foreach(technique => {
          println(s"INSERT INTO instrument_scientific_technique (instrument_id, scientific_technique_id) VALUES (currval('instrument_id_seq'::regclass), (select id from scientific_technique where name = '${technique.toLowerCase}'));")
        })
      })
    })
  }
}
