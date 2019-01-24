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
package eu.ill.puma.sparkmatcher.utils.nlp.FormulaAnalyser.service;

import eu.ill.puma.sparkmatcher.utils.nlp.FormulaAnalyser.entities.Word;
import eu.ill.puma.sparkmatcher.utils.nlp.FormulaAnalyser.entities.WordList;

import java.util.Arrays;

public class FormulaAnalyserService {
    public static WordList analyse(String textToAnalyse, String blackList) {
        //parse fulltext
        WordList words = WordService.convert(Arrays.asList(textToAnalyse.split("[\\s+|\\n]|–")));

        //remove english words
        for (Word word : words) {
            WordService.normalize(word);
            WordService.classify(word, blackList);
        }

        return words;
    }

    public static Word analyserSingleWord(String wordString, String blackList) {
        //parse fulltext
        Word word = new Word(wordString);
        WordService.normalize(word);
        WordService.classify(word, blackList);

        return word;
    }
}
