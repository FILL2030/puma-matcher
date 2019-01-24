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
package eu.ill.puma.sparkmatcher.utils

import scala.collection.mutable.ListBuffer

object StringUtils {

	def join(list: List[String], joinValue: String): String = {
		var output: String = ""
		val size = list.size
		var index = 0
		list.foreach(value => {
			output += value
			if (index < size - 1) {
				output += joinValue
			}
			index += 1
		})

		output
	}

	def join(list: List[String]): String = join(list, ", ")

	def rejoin(word: String, oldJoinValue: String, newJoinValue: String): String = {
		var newWord = ""
		if (oldJoinValue.length > 1) {
			newWord = word.replaceAll(oldJoinValue, newJoinValue)

		} else {
			newWord = word.replaceAll("\\" + oldJoinValue, newJoinValue)

		}
		val parts = newWord.split(newJoinValue)
		val validParts = ListBuffer[String]()

		parts.foreach(part => {
			if (part.length > 0) {
				validParts += part
			}
		})
		join(validParts.toList, newJoinValue)
	}

	def firstWord(value: String): String = {
		if (value.contains(' ')) {
			value.substring(0, value.indexOf(' '))

		} else {
			value
		}
	}

}
