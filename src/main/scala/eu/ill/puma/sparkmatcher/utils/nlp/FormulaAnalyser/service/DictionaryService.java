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

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DictionaryService {
    private String sparkRessourceRoot = System.getenv("SPARK_RESOURCE_ROOT");

    private static DictionaryService instance = new DictionaryService();

    public static DictionaryService getInstance() {
        return instance;
    }

    private boolean status = true;

    private Set<String> englishWords = new HashSet();

    private Set<String> names = new HashSet();

    private Set<String> cities = new HashSet();

    private Set<String> countries = new HashSet();

    private Set<String> instrumentCodes = new HashSet();

    private DictionaryService() {
        try {
            String englishWordsString = FileUtils.readFileToString(new File(sparkRessourceRoot + "/english_dictionary_large.txt"));
            englishWords.addAll(Arrays.asList(englishWordsString.toLowerCase().split("\n")));

            String namesString = FileUtils.readFileToString(new File(sparkRessourceRoot + "names.txt"));
            names.addAll(Arrays.asList(namesString.toLowerCase().split("\n")));

            String cityString = FileUtils.readFileToString(new File(sparkRessourceRoot + "cities.txt"));
            cities.addAll(Arrays.asList(cityString.toLowerCase().split("\n")));

            String countriesString = FileUtils.readFileToString(new File(sparkRessourceRoot + "countries.txt"));
            countries.addAll(Arrays.asList(countriesString.toLowerCase().split("\n")));

            String instrumentString = FileUtils.readFileToString(new File(sparkRessourceRoot + "instrumentList.txt"));
            instrumentCodes.addAll(Arrays.asList(countriesString.toLowerCase().split("\n")));

        } catch (Exception e) {
            status = false;
        }
    }

    public boolean getStatus() {
        return status;
    }

    public Set<String> getEnglishWords() {
        return englishWords;
    }

    public Set<String> getNames() {
        return names;
    }

    public Set<String> getCities() {
        return cities;
    }

    public Set<String> getCountries() {
        return countries;
    }

    public Set<String> getInstrumentCodes() {
        return instrumentCodes;
    }
}
