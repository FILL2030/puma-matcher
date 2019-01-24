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

object StringComparer {

	private val EMAIL_PATTERN: String = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6}$"

	def nullOrEmpty(value: String): Boolean = {

		value == null || value.length == 0
	}

	def nullOrShorterThan(value: String, length: Int): Boolean = {

		value == null || value.length < length
	}

	def isEmail(value: String): Boolean = {
		nullOrEmpty(value) || value.matches(EMAIL_PATTERN)
	}

	def startsOrEndsWith(value: String, part: String): Boolean = {
		value.startsWith(part) || value.endsWith(part)
	}

	def areEqual(value1: String, value2: String): Boolean = {
		if (nullOrEmpty(value1)) {
			false

		} else if (nullOrEmpty(value2)) {
			false

		} else {
			value1 == value2
		}
	}

	def areEqualOrNull(value1: String, value2: String): Boolean = {
		if (value1 == null && value2 == null) {
			true

		} else if (value1 != null && value2 != null) {
			value1 == value2

		} else {
			false
		}
	}

	def areAllNullOrEmpty(values: String*): Boolean = {
		var allNullOrEmpty = true
		values.foreach(value => {
			if (!nullOrEmpty(value)) {
				allNullOrEmpty = false
			}
		})

		allNullOrEmpty
	}

	def areAnyNullOrEmpty(values: String*): Boolean = {
		var anyNullOrEmpty = false
		values.foreach(value => {
			if (nullOrEmpty(value)) {
				anyNullOrEmpty = true
			}
		})

		anyNullOrEmpty
	}

	def areNotNullAndNotEmptyAndFirstLetterEqual(values: String*): Boolean = {
		var anyNullOrEmpty = false
		values.foreach(value => {
			if (nullOrEmpty(value)) {
				anyNullOrEmpty = true
			}
		})

		if (anyNullOrEmpty) {
			false

		} else {
			val firstChar = values(0).charAt(0)
			var isFirst = true
			var firstLettersEqual = true

			values.foreach(value => {
				if (isFirst) {
					isFirst = false
				} else {
					if (value.charAt(0) != firstChar) {
						firstLettersEqual = false
					}
				}
			})

			firstLettersEqual
		}
	}


}
