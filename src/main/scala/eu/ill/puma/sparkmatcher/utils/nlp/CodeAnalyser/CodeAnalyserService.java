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
package eu.ill.puma.sparkmatcher.utils.nlp.CodeAnalyser;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CodeAnalyserService {
	private static Pattern proposalCodeRegex = Pattern.compile("[1-9]-[0-9]{1,2}-[0-9]{1,4}|CRG-[0-9]{2,4}|TEST-[0-9]{1,4}|INTER-[0-9]{1,4}|BAG-[0-9]{1,2}-[0-9]{1,4}|EASY-[0-9]{1,4}|DL-[0-9]{1,2}-[0-9]{1,4}|LTP-[0-9]{1,2}[A-Z]{0,1}-[0-9]{1,4}|DIR-[0-9]{1,3}|UGA-[0-9]{1,3}|INDU-[0-9]{1,3}|ST-[0-9]{1,3}|DEUT-[0-9]{1,3}");
	private static Pattern doiRegex = Pattern.compile("\\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&\\'<>])\\S)+)\\b");

	public static List<String> analyseCode(String text) {

		text = text.replaceAll("\n", "");
		text = text.replaceAll("\r", "");
		text = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");

		Matcher proposalCodeMatcher = proposalCodeRegex.matcher(text);

		List<String> codes = new ArrayList();

		while (proposalCodeMatcher.find()) {
			String code = proposalCodeMatcher.group();

			if (code.length() > 4) {
				codes.add(code);
			}
		}

		return codes;
	}

	public static List<String> analyseDoi(String text) {

		text = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");

		Matcher doiMatcher = doiRegex.matcher(text);

		List<String> dois = new ArrayList();

		while (doiMatcher.find()) {
			dois.add(doiMatcher.group(0));
		}

		return dois;
	}
}
