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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CodeAnalyserService {

    private static String proposalCodeRegexString = "[1-9]-[0-9]{1,2}-[0-9]{1,4}|CRG-[0-9]{2,4}|TEST-[0-9]{1,4}|INTER-[0-9]{1,4}|BAG-[0-9]{1,2}-[0-9]{1,4}|EASY-[0-9]{1,4}|DL-[0-9]{1,2}-[0-9]{1,4}|LTP-[0-9]{1,2}[A-Z]{0,1}-[0-9]{1,4}|DIR-[0-9]{1,3}|UGA-[0-9]{1,3}|INDU-[0-9]{1,3}|ST-[0-9]{1,3}|DEUT-[0-9]{1,3}";
    private static Pattern proposalCodeRegex = Pattern.compile(proposalCodeRegexString);
    private static Pattern doiRegex = Pattern.compile("\\b(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&\\'<>])\\S)+)\\b");

    private static final int illDoiSentenceOffSet = 20;

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

    public static List<String> analyseILLDoi(String text) {

        text = Normalizer.normalize(text, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");

        Matcher doiMatcher = doiRegex.matcher(text);

        List<String> dois = new ArrayList();

        while (doiMatcher.find()) {
            //match doi
            String doi = doiMatcher.group(0);

            //handle ILL data DOI
            if (doi.contains("10.5291/ILL")) {
                //extract words after doi if exist
                int start = doiMatcher.start(0);
                int end = doiMatcher.end(0) + +illDoiSentenceOffSet;

                if (end >= text.length()) {
                    end = text.length();
                }

                String doiSentence = text.substring(start, end);

                //check if the word after the doi is a part of the proposal code
                List<String> wordsAfterDoi = Arrays.asList(doiSentence.substring(doi.length()).split(" "))
                        .stream().filter(word -> word.length() > 0)
                        .map(word -> {
                            if (word.endsWith(".") || word.endsWith(",") || word.endsWith(";")) {
                                return word.substring(0, word.length() - 1);
                            } else {
                                return word;
                            }
                        })
                        .filter(word -> word.length() > 0)
                        .filter(word -> !word.equals("-"))
                        .collect(Collectors.toList());


                List<String> doiProposalCode = Arrays.asList(doi.split("\\.")).stream().filter(word -> word.length() > 0).collect(Collectors.toList());

                //check list are not empty
                if (wordsAfterDoi.size() > 0 && doiProposalCode.size() > 0) {

                    //build potential doi
                    String potentialProposalCode = doiProposalCode.get(doiProposalCode.size() - 1) + wordsAfterDoi.get(0);

                    //validation
                    if (potentialProposalCode.matches(proposalCodeRegexString)) {
                        doi = doi + wordsAfterDoi.get(0);
                    }
                }

                //check list are not empty
                if (wordsAfterDoi.size() > 1 && doiProposalCode.size() > 0) {

                    //build potential doi
                    String potentialProposalCode = doiProposalCode.get(doiProposalCode.size() - 1) + wordsAfterDoi.get(0) + wordsAfterDoi.get(1);

                    //validation
                    if (potentialProposalCode.matches(proposalCodeRegexString)) {
                        doi = doi + wordsAfterDoi.get(0) + wordsAfterDoi.get(1);
                    }
                }

                //check list are not empty
                if (wordsAfterDoi.size() > 0) {

                    //build potential doi
                    String potentialProposalCode = wordsAfterDoi.get(0);

                    //validation
                    if (potentialProposalCode.matches(proposalCodeRegexString)) {
                        doi = doi + wordsAfterDoi.get(0);
                    }
                }

                //check list are not empty
                if (wordsAfterDoi.size() > 0 &&  wordsAfterDoi.get(0).split("\\.").length > 1) {

                    //build potential doi
                    String data = wordsAfterDoi.get(0).split("\\.")[0];
                    String potentialProposalCode = wordsAfterDoi.get(0).split("\\.")[1];


                    //validation
                    if (potentialProposalCode.matches(proposalCodeRegexString) && data.equals("DATA")) {
                        doi = doi + "DATA." + potentialProposalCode;
                    }

                    //validation
                    if (potentialProposalCode.matches(proposalCodeRegexString) && data.equals("-DATA")) {
                        doi = doi + "-DATA." + potentialProposalCode;
                    }
                }

                //check list are not empty
                if (wordsAfterDoi.size() > 2) {

                    //build potential doi
                    String potentialProposalCode = wordsAfterDoi.get(0) + wordsAfterDoi.get(1) + wordsAfterDoi.get(1);

                    //validation
                    if (potentialProposalCode.matches(proposalCodeRegexString)) {
                        doi = doi + wordsAfterDoi.get(0) + wordsAfterDoi.get(1) + wordsAfterDoi.get(1);
                    }
                }

            }

            dois.add(doi);

        }


        return dois;
    }

//    public static void main(String[] args) {
//        CodeAnalyserService.analyseILLDoi("bla fldsmfl fdshkfks 10.5291/ILL -DATA.8-76-780 gljfdsjgfklj kjgfsdlgk").forEach(doi -> System.out.println(doi));
//    }
}
