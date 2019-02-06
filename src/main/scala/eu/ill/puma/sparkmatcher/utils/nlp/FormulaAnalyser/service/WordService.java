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
import eu.ill.puma.sparkmatcher.utils.nlp.FormulaAnalyser.entities.WordType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WordService {

    private static Pattern emailRegex = Pattern.compile("^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$", Pattern.CASE_INSENSITIVE);

    private static Pattern doublePattern = Pattern.compile("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?");

    private static Pattern doiRegex = Pattern.compile("(10[.][0-9]{4,}(?:[.][0-9]+)*/(?:(?![\"&\\'<>])\\S)+)");

    private static Pattern ipAddrRegex = Pattern.compile("\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b");

    private static Pattern formulaElementRegex = Pattern.compile("((\\d*)((Uut|Uup|Uus|Uuo|He|Li|Be|Ne|Na|Mg|Al|Si|Cl|Ar|Ca|Sc|Ti|Cr|Mn|Fe|Co|Ni|Cu|Zn|Ga|Ge|As|Se|Br|Kr|Rb|Sr|Zr|Nb|Mo|Tc|Ru|Rh|Pd|Ag|Cd|In|Sn|Sb|Te|Xe|Cs|Ba|La|Ce|Pr|Nd|Pm|Sm|Eu|Gd|Tb|Dy|Ho|Er|Tm|Yb|LuHf|Ta|Re|Os|Ir|Pt|Au|Hg|Tl|Pb|Bi|Po|At|Rn|Fr|Ra|Ac|Th|Pa|Np|Pu|Am|Cm|Bk|Cf|Es|Fm|Md|No|Lr|Rf|Db|Sg|Bh|Hs|Mt|Ds|Rg|Cn|Fl|Lv|H|B|C|N|O|F|P|S|K|V|Y|I|W|U|\\(|\\)|\\*|\\[|\\])\\d*)+)");

    private static Pattern formulaFragmentRegex = Pattern.compile("((Uut|Uup|Uus|Uuo|He|Li|Be|Ne|Na|Mg|Al|Si|Cl|Ar|Ca|Sc|Ti|Cr|Mn|Fe|Co|Ni|Cu|Zn|Ga|Ge|As|Se|Br|Kr|Rb|Sr|Zr|Nb|Mo|Tc|Ru|Rh|Pd|Ag|Cd|In|Sn|Sb|Te|Xe|Cs|Ba|La|Ce|Pr|Nd|Pm|Sm|Eu|Gd|Tb|Dy|Ho|Er|Tm|Yb|LuHf|Ta|Re|Os|Ir|Pt|Au|Hg|Tl|Pb|Bi|Po|At|Rn|Fr|Ra|Ac|Th|Pa|Np|Pu|Am|Cm|Bk|Cf|Es|Fm|Md|No|Lr|Rf|Db|Sg|Bh|Hs|Mt|Ds|Rg|Cn|Fl|Lv|H|B|C|N|O|F|P|S|K|V|Y|I|W|U)\\d{0,3})+");

    private static Pattern specialFormulaFragmentRegex = Pattern.compile("((Uut|Uup|Uus|Uuo|He|Li|Be|Ne|Na|Mg|Al|Si|Cl|Ar|Ca|Sc|Ti|Cr|Mn|Fe|Co|Ni|Cu|Zn|Ga|Ge|As|Se|Br|Kr|Rb|Sr|Zr|Nb|Mo|Tc|Ru|Rh|Pd|Ag|Cd|In|Sn|Sb|Te|Xe|Cs|Ba|La|Ce|Pr|Nd|Pm|Sm|Eu|Gd|Tb|Dy|Ho|Er|Tm|Yb|LuHf|Ta|Re|Os|Ir|Pt|Au|Hg|Tl|Pb|Bi|Po|At|Rn|Fr|Ra|Ac|Th|Pa|Np|Pu|Am|Cm|Bk|Cf|Es|Fm|Md|No|Lr|Rf|Db|Sg|Bh|Hs|Mt|Ds|Rg|Cn|Fl|Lv|H|B|C|N|O|F|P|S|K|V|Y|I|W|U)\\d{0,4})+");

    private static Pattern formulaElementWithOneCharRegex = Pattern.compile("(HBCNOFPSKVYIWU)+");

    private static Pattern isotopeRegex1 = Pattern.compile("(\\d{0,3}(Uut|Uup|Uus|Uuo|He|Li|Be|Ne|Na|Mg|Al|Si|Cl|Ar|Ca|Sc|Ti|Cr|Mn|Fe|Co|Ni|Cu|Zn|Ga|Ge|As|Se|Br|Kr|Rb|Sr|Zr|Nb|Mo|Tc|Ru|Rh|Pd|Ag|Cd|In|Sn|Sb|Te|Xe|Cs|Ba|La|Ce|Pr|Nd|Pm|Sm|Eu|Gd|Tb|Dy|Ho|Er|Tm|Yb|LuHf|Ta|Re|Os|Ir|Pt|Au|Hg|Tl|Pb|Bi|Po|At|Rn|Fr|Ra|Ac|Th|Pa|Np|Pu|Am|Cm|Bk|Cf|Es|Fm|Md|No|Lr|Rf|Db|Sg|Bh|Hs|Mt|Ds|Rg|Cn|Fl|Lv|H|B|C|N|O|F|P|S|K|V|Y|I|W|U))");
    private static Pattern isotopeRegex2 = Pattern.compile("((Uut|Uup|Uus|Uuo|He|Li|Be|Ne|Na|Mg|Al|Si|Cl|Ar|Ca|Sc|Ti|Cr|Mn|Fe|Co|Ni|Cu|Zn|Ga|Ge|As|Se|Br|Kr|Rb|Sr|Zr|Nb|Mo|Tc|Ru|Rh|Pd|Ag|Cd|In|Sn|Sb|Te|Xe|Cs|Ba|La|Ce|Pr|Nd|Pm|Sm|Eu|Gd|Tb|Dy|Ho|Er|Tm|Yb|LuHf|Ta|Re|Os|Ir|Pt|Au|Hg|Tl|Pb|Bi|Po|At|Rn|Fr|Ra|Ac|Th|Pa|Np|Pu|Am|Cm|Bk|Cf|Es|Fm|Md|No|Lr|Rf|Db|Sg|Bh|Hs|Mt|Ds|Rg|Cn|Fl|Lv|H|B|C|N|O|F|P|S|K|V|Y|I|W|U)\\d{0,3})");

    private static Pattern alphaNumRegex = Pattern.compile("(\\d|[A-Z]|[a-z])+");

    private static Pattern upperCaseCharRegex = Pattern.compile("[A-Z]+");


    public static Word normalize(Word word) {
        if (!word.isNormalized()) {

            word.removeIfEndBy("-");
            word.removeIfEndBy("\\");
            word.removeIfEndBy(",");
            word.removeIfEndBy(".");
            word.removeIfEndBy(":");
            word.removeIfEndBy(";");
            word.removeIfEndBy("'s");
            word.removeIfEndBy("‘");
            word.removeIfEndBy("’");
            word.removeIfStartBy("'");
            word.removeIfStartBy("\"");
            word.removeIfStartBy("^");
            word.removeIfStartBy("{");
            word.removeIfStartBy("}");
            word.removeIfStartBy("/");

            word.removeIfStartBy("-");
            word.removeIfStartBy("\\");
            word.removeIfStartBy(",");
            word.removeIfStartBy(".");
            word.removeIfStartBy(":");
            word.removeIfStartBy(";");
            word.removeIfStartBy("‘");
            word.removeIfEndBy("'");
            word.removeIfEndBy("\"");

            word.replace("\u0000", "");
            word.replace("\u0001", "");
            word.replace("\u0002", "");
            word.replace("\u0003", "");
            word.replace("\u0004", "");
            word.replace("\u0005", "");
            word.replace("\u0006", "");
            word.replace("\u0007", "");
            word.replace("\u0010", "");
            word.replace("\u0011", "");
            word.replace("\u0012", "");
            word.replace("\u0013", "");
            word.replace("\u0014", "");
            word.replace("\u0015", "");
            word.replace("\u0016", "");
            word.replace("\u0017", "");
            word.replace("\u000E", "");
            word.replace("\u000F", "");
            word.replace("\u001E", "");
            word.replace("\u001D", "");
            word.replace("\u001F", "");
            word.replace("\b", "");

            if (word.beginBy("(") && word.endWith(")") && StringUtils.countMatches(word.getCleanWord(), "(") == 1 && StringUtils.countMatches(word.getCleanWord(), ")") == 1) {
                word.removeIfStartBy("(");
                word.removeIfEndBy(")");
            }

            if (word.beginBy("[") && word.endWith("]")) {
                word.removeIfStartBy("[");
                word.removeIfEndBy("]");
            }

            if (word.isNormalized() == false) {
                word.setNormalized();
                word = normalize(word);
            }
        }
        return word;
    }

    public static WordList convert(List<String> words) {
        WordList wordList = new WordList();

        for (String word : words) {
            wordList.add(new Word(word));
        }

        return wordList;
    }

    public static Word classify(Word word, String blackList) {


        //long word
        if (word.getOriginalWordLength() > 30) {
            word.addType(WordType.other);
        }

        //english word
        if (DictionaryService.getInstance().getEnglishWords().contains(word.getCleanWord().toLowerCase())) {
            word.addType(WordType.englishWord);
        }

        //name
        if (DictionaryService.getInstance().getNames().contains(word.getCleanWord().toLowerCase())) {
            word.addType(WordType.name);
        }

        //cities
        if (DictionaryService.getInstance().getCities().contains(word.getCleanWord().toLowerCase())) {
            word.addType(WordType.city);
        }

        //name
        if (DictionaryService.getInstance().getCountries().contains(word.getCleanWord().toLowerCase())) {
            word.addType(WordType.country);
        }

        //instrument code
        if (DictionaryService.getInstance().getInstrumentCodes().contains(word.getCleanWord().toLowerCase())) {
            word.addType(WordType.instrument_code);
        }

        //short word
        if (word.getCleanWordLength() < 5) {
            word.addType(WordType.shortWord);
        }

        //number
        if (NumberUtils.isNumber(word.getCleanWord().replaceAll("\\(", "").replaceAll("\\)", ""))) {
            word.addType(WordType.number);
        }

        try {
            Integer.parseInt(word.getCleanWord());
            word.addType(WordType.number);
        } catch (Exception e) {
        }

        //composed english word
        if (word.getCleanWord().contains("-") || word.getCleanWord().contains("–") || word.getCleanWord().contains("_")) {
            String[] wordParts = word.getCleanWord().split("[-|–|_]");

            boolean isComposedWord = true;
            for (int i = 0; i < wordParts.length; i++) {
                if (!DictionaryService.getInstance().getEnglishWords().contains(wordParts[i].toLowerCase())) {
                    isComposedWord = false;
                }
            }

            if (isComposedWord) {
                word.addType(WordType.composedWord);
                word.addType(WordType.englishWord);
            }
        }

        //email
        Matcher emailMatcher = emailRegex.matcher(word.getCleanWord());
        if (emailMatcher.matches()) {
            word.addType(WordType.email);
        }

        //url
        if (word.getCleanWord().contains("http://") || word.getCleanWord().contains("https://")) {
            word.addType(WordType.url);
        }

        //doi
        Matcher doiMatcher = doiRegex.matcher(word.getCleanWord());
        if (doiMatcher.find()) {
            word.addType(WordType.doi);
        }

        //ip addr
        Matcher ipAddrMarcher = ipAddrRegex.matcher(word.getCleanWord());
        if (ipAddrMarcher.matches()) {
            word.addType(WordType.ip);
        }

        //formula
        if (word.hasType(WordType.unknown)) {
            boolean potentialFormula = false;

            Matcher alphaNumericMatcher = alphaNumRegex.matcher(word.getCleanWord());
            String alphaNumericWord = "";
            while (alphaNumericMatcher.find()) {
                alphaNumericWord += alphaNumericMatcher.group();
            }

            Matcher fragmentMatcher = formulaFragmentRegex.matcher(word.getCleanWord());
            Matcher specialFragmentMatcher = specialFormulaFragmentRegex.matcher(alphaNumericWord);
            Matcher upperCaseMatcher = upperCaseCharRegex.matcher(word.getCleanWord());
            Matcher formulaElementWithOneCharMatcher = formulaElementWithOneCharRegex.matcher(word.getCleanWord());

            Matcher isotopeMatcher1 = isotopeRegex1.matcher(word.getCleanWord());
            Matcher isotopeMatcher2 = isotopeRegex2.matcher(word.getCleanWord());

            //full formula (YBa2Cu3O6)
            {
                String fragmentString = "";
                while (fragmentMatcher.find()) {
                    fragmentString += fragmentMatcher.group();
                }

                if (fragmentMatcher.matches() && fragmentMatcher.start() == 0 && fragmentString.length() == word.getCleanWord().length()) {
                    if (upperCaseMatcher.matches() && upperCaseMatcher.start() == 0 && upperCaseMatcher.end() == word.getCleanWord().length()) {
                        if (formulaElementWithOneCharMatcher.matches() && formulaElementWithOneCharMatcher.start() == 0 && formulaElementWithOneCharMatcher.end() == word.getCleanWord().length()) {
                            potentialFormula = true;
                        }
                    } else {
                        potentialFormula = true;
                    }
                }
            }

            //composed formula ((La0.63Ca0.37)MnO3
            {
                fragmentMatcher = fragmentMatcher.reset();
                Set<String> fragments = new HashSet();

                while (fragmentMatcher.find()) {
                    if (formulaElementRegex.matcher(fragmentMatcher.group()).matches()) {
                        fragments.add(fragmentMatcher.group());
                    }
                }

                String matchString = "";
                for (String fragment : fragments) {
                    matchString += fragment;
                }

                if (matchString.length() > word.getCleanWord().length() * 2 / 3) {
                    potentialFormula = true;
                }
            }

            //formula with special char
            {
                Set<String> fragments = new HashSet();

                while (specialFragmentMatcher.find()) {
                    if (formulaElementRegex.matcher(specialFragmentMatcher.group()).matches()) {
                        fragments.add(specialFragmentMatcher.group());
                    }
                }

                String matchString = "";
                for (String fragment : fragments) {
                    matchString += fragment;
                }

                if (matchString.length() > alphaNumericWord.length() * 3 / 4) {
                    potentialFormula = true;
                }
            }

            //isotope (647Pb)
            if (isotopeMatcher1.matches() && isotopeMatcher1.start() == 0 && isotopeMatcher1.end() == word.getCleanWord().length()) {
                potentialFormula = true;
            }

            if (isotopeMatcher2.matches() && isotopeMatcher2.start() == 0 && isotopeMatcher2.end() == word.getCleanWord().length()) {
                potentialFormula = true;
            }


            //final validation
            upperCaseMatcher = upperCaseMatcher.reset();
            formulaElementWithOneCharMatcher = formulaElementWithOneCharMatcher.reset();

            String upperCaseString = "";
            while (upperCaseMatcher.find()) {
                upperCaseString += upperCaseMatcher.group();
            }

            String upperCaseFormulaString = "";
            while (formulaElementWithOneCharMatcher.find()) {
                upperCaseFormulaString += formulaElementWithOneCharMatcher.group();
            }

            //handle full uppercase word with some letter which are not formula letter eg: E in BUNSEKI
            if (upperCaseString.length() == alphaNumericWord.length()) {// full word uppercase

                //detect if some letter can not be in formula
                if (upperCaseFormulaString.length() < alphaNumericWord.length()) {
                    potentialFormula = false;
                }

                //special length control
                if (alphaNumericWord.length() <= 4) {
                    potentialFormula = false;
                }
            }

            //forbidden character
            if (word.getCleanWord().contains("!") ||
                    word.getCleanWord().contains("?") ||
                    word.getCleanWord().contains("§") ||
                    word.getCleanWord().contains("ù") ||
                    word.getCleanWord().contains("%") ||
                    word.getCleanWord().contains("£") ||
                    word.getCleanWord().contains("à") ||
                    word.getCleanWord().contains("@") ||
                    word.getCleanWord().contains("ç") ||
                    word.getCleanWord().contains("#") ||
                    word.getCleanWord().contains("€") ||
                    word.getCleanWord().contains("é") ||
                    word.getCleanWord().contains("=") ||
                    word.getCleanWord().contains("&") ||
                    word.getCleanWord().contains("III") ||
                    word.getCleanWord().contains("II") ||
                    word.getCleanWord().contains("000") ||
                    word.getCleanWord().contains("$")) {
                potentialFormula = false;
            }

            //length control
            if (alphaNumericWord.length() < 4) {
                potentialFormula = false;
            }

            //parenthesis control
            if (word.isCountEquals("(", ")") == false) {
                potentialFormula = false;
            }

            //first letter should be uppercase
            if (alphaNumericWord.length() > 0 && Character.isLowerCase(alphaNumericWord.charAt(0))) {
                potentialFormula = false;
            }

            //black : eg postcode
            if (blackList.contains(alphaNumericWord)) {
                potentialFormula = false;
            }

            //check if the number of numeric char is not higher than the the of letter
            int digitCount = 0;
            int letterCount = 0;
            int wordSize = alphaNumericWord.length();
            int numberOfNumber = Arrays.stream(alphaNumericWord.split("[A-Z]|[a-z]+"))
                    .filter(s -> s.matches("(.+)?[0-9](.+)?"))
                    .sorted((s1, s2) -> s2.length() - s1.length())
                    .collect(Collectors.toList())
                    .size();


            for (int i = 0, len = wordSize; i < len; i++) {
                if (Character.isDigit(alphaNumericWord.charAt(i))) digitCount++;
            }

            for (int i = 0, len = wordSize; i < len; i++) {
                if (Character.isLetter(alphaNumericWord.charAt(i))) letterCount++;
            }

            //offset of 1 to keep Ur235
            if (digitCount > letterCount + numberOfNumber) {
                potentialFormula = false;
            }




            //mark as formula
            if (potentialFormula) {
                word.addType(WordType.formula);
            }
        }

        return word;
    }

//    public static void main(String[] args){
//        String testFormula = "F-76432";
//        Word word = new Word(testFormula);
//        word = WordService.classify(word, "");
//
//        System.out.println(word.getTypes());
//    }
}
