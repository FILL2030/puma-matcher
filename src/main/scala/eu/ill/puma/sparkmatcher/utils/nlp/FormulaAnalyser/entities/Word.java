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
package eu.ill.puma.sparkmatcher.utils.nlp.FormulaAnalyser.entities;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

public class Word {
	private String originalWord;

	private String cleanWord;

	private List<WordType> types = new ArrayList();

	private boolean normalized = false;

	public Word(String originalWord) {
		this.originalWord = originalWord;
		this.cleanWord = Normalizer.normalize(originalWord, Normalizer.Form.NFD);
		this.cleanWord = cleanWord.replaceAll("[^\\x00-\\x7F]", "");
		this.types.add(WordType.unknown);
	}

	public boolean beginBy(String start) {
		if (cleanWord.length() >= start.length()) {
			for (int i = 0; i < start.length(); i++) {
				if (cleanWord.charAt(i) != start.charAt(i)) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}

	public boolean endWith(String end) {
		int diff = cleanWord.length() - end.length();

		if (diff > 0) {
			for (int i = cleanWord.length(); i > diff; i--) {
				if (cleanWord.charAt(i - 1) != end.charAt(i - diff - 1)) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}

	public String removeIfStartBy(String start) {
		if (this.beginBy(start)) {
			cleanWord = cleanWord.substring(start.length(), cleanWord.length());
		}

		return cleanWord;
	}

	public String removeIfEndBy(String end) {
		if (this.endWith(end)) {
			cleanWord = cleanWord.substring(0, cleanWord.length() - end.length());
		}

		return cleanWord;
	}

	public void replace(String stringToReplace, String replacement) {
		cleanWord = cleanWord.replace(stringToReplace, replacement);
	}

	public boolean isCountEquals(String p1, String p2) {
		int opened = StringUtils.countMatches(this.getCleanWord(), p1);
		int closed = StringUtils.countMatches(this.getCleanWord(), p2);

		return opened == closed;
	}

	public int getOriginalWordLength() {
		return this.originalWord.length();
	}

	public int getCleanWordLength() {
		return this.cleanWord.length();
	}

	public String getOriginalWord() {
		return originalWord;
	}

	public String getCleanWord() {
		return cleanWord;
	}

	public void addType(WordType type) {
		if (this.types.contains(WordType.unknown)) {
			this.types.remove(WordType.unknown);
		}

		this.types.add(type);
	}

	public boolean hasType(WordType type) {
		return this.types.contains(type);
	}


	public List<WordType> getTypes() {
		return types;
	}

	public void setNormalized() {
		this.cleanWord = Normalizer.normalize(this.cleanWord, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
		this.normalized = true;
	}

	public boolean isNormalized() {
		return normalized;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (o == null || getClass() != o.getClass()) return false;

		Word word = (Word) o;

		return new EqualsBuilder()
				.append(originalWord, word.originalWord)
				.append(cleanWord, word.cleanWord)
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(originalWord)
				.append(cleanWord)
				.toHashCode();
	}
}
