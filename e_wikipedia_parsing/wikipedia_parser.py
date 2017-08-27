# -*- coding: utf-8 -*-

import sys
import string
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pattern.de import parse, split
import regex
sys.path.append(".")
sys.path.append("treetagger")

from utils import *
import nltk
from Trie import Trie
#from e_wikipedia_parsing.Trie import Trie
from treetagger_python2 import TreeTagger
from pattern.de import split as split_de
from pattern.de import parse as parse_de
from pattern.fr import split as split_fr
from pattern.fr import parse as parse_fr
from pattern.en import split as split_en
from pattern.en import parse as parse_en
from pattern.it import split as split_it
from pattern.it import parse as parse_it

class Wikipedia_parser(object):
    def __init__(self,parameters, language=None, row=None,dataset_method=None,wp_to_ner_by_title=None, alternative_titles=None, personal_titles=None,
                 sentence_starter=None, section_to_remove=None, allowed_with_cap=None,nationalities={}, calendar={}):
        self.language = language
        self.main_language = get_main_language(language)
        self.full_name_language = get_full_name_language(language)
        self.text = " ".join(row['text'].split("\n\n")[1:]) #each article always start with title\n\n and then first sentence.
        self.title = row['title']
        self.out_links = row['out_links']
        self.indexes_to_link = dict()
        self.PERSONAL_TITLES = personal_titles
        self.NATIONALITIES = nationalities
        self.alternative_titles = alternative_titles
        self.dataset_method = dataset_method
        self.SENTENCE_STARTER = sentence_starter
        self.section_to_remove = section_to_remove
        self.wp_to_ner_by_title = wp_to_ner_by_title
        self.ALLOWED_WITH_CAP = allowed_with_cap
        self.CALENDAR = calendar
        self.nltk_folder = parameters['nltk_folder']

    def is_redirect_(self):
        return "#REDIRECT" in self.text

    def parse(self):
        subpart = self._map_title(self.text)
        subpart = self._map_link(subpart)
        self.parsed_text = self._parse(subpart)
        self.parsed_text = self.build_dataset(self.parsed_text)
        print(self.parsed_text)

    def parse_spark(self):
        text = self._map_title(self.text)
        text = self._map_link(text)
        sequence = self._parse(text)
        self.text = self.build_dataset(sequence)
        return Row(title=self.title, text=self.text)

    @staticmethod
    def test():
        return Row(title="title", text="a")

    def parse_wikiExt(self):
        #small preprocessing
        #return Row(title="title", text="a")
        text = self.small_preprocessing(self.text)
        text = self._map_title(text)
        text = self._map_link(text)
        sequence = self._parse(text)
        text = self.build_dataset(sequence)
        return Row(title=self.title, text=text)

    def small_preprocessing(self, text):
        for section in self.section_to_remove:
            text = text[:text.find(section+".\n")]
        #replace the %20 by space:
        text = text.replace("%20", " ")
        text = text.replace("%2C", ",")
        text = self.remove_empty_parenthesis(text)
        return text

    @staticmethod
    def remove_empty_parenthesis(text):
        empty_parenthesis = r"\(\s*?\)"
        return regex.sub(empty_parenthesis, "", text)

    def _map_link(self, text):
        #reg = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
        reg = r"<a href=\"(.*?)\">(.*?)</a>"

        result = regex.finditer(reg, text)
        output = text

        for r in result:
            # [[Algorithm#Examples]]
            if "#" in r.group(2):
                replace_with = r.group(1).split('#')[0]
                output = output.replace(text[r.start():r.end()], replace_with, 1)
            else:
                replace_with = r.group(1)
                link = r.group(1)
                start = output.find(text[r.start():r.end()])
                self.indexes_to_link[start] = ((start, start + len(replace_with)), link)
                output = output.replace(text[r.start():r.end()], replace_with, 1)
        return output
    """
    def _map_link(self, text):
        reg = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"

        result = regex.finditer(reg, text)
        output = text

        for r in result:
            # [[Algorithm#Examples]]
            if "#" in r.group(1):
                replace_with = r.group(1).split('#')[0]
                output = output.replace(text[r.start():r.end()], replace_with, 1)
            else:
                replace_with = r.group(1) if r.group(3) is None else r.group(3)
                link = r.group(1)
                start = output.find(text[r.start():r.end()])
                self.indexes_to_link[start] = ((start, start + len(replace_with)),link)
                output = output.replace(text[r.start():r.end()], replace_with, 1)
        return output

    """

    def _map_title(self, text):
        reg = r"\"\'([\S\s]*?)\"\'" #r"\'\'\'([\S\s]*?)\'\'\'"
        reg = regex.compile(reg)
        result = reg.finditer(text, endpos=1500)
        output = text
        self.title_list = []
        for r in result:
            replace_with = r.group(1)
            self.title_list.append(replace_with)
            start = output.find(text[r.start():r.end()])
            self.indexes_to_link[start] = ((start, start + len(replace_with)),self.title)
            output = output.replace(text[r.start():r.end()], replace_with, 1)
        return output

    @staticmethod
    def final_cleaning(text):
        #reg = r"``(.*?)\""
        #text = regex.sub(reg, r"\1", text)
        #reg = r"``(.*?)\'"
        #text = regex.sub(reg, r"\1", text)
        text = text.replace("``", "")
        text = text.replace("''", "")
        text = regex.sub(r"„“", "", text)

        return text

    def transform_parsing(self, sentence):
        output_sentence = []
        for w in sentence.words:
            output_sentence.append((w.string, w.pos))#, w.lemma))
        return output_sentence

    def _parse(self,text):
        text = self.final_cleaning(text)
        #parsed_data = nltk.sent_tokenize(text, language=self.full_name_language)
        #tokenizer = nltk.data.load(os.path.join(self.nltk_folder,"tokenizers/punkt/{}.pickle".format(self.full_name_language)))
        #parsed_data = tokenizer.tokenize(text)
        #if self.language == "en":
        #    parsed_data = nltk.sent_tokenize(text, language=self.language)
        #elif self.language == "de" or self.language == "als":

        if self.main_language == "en":
            parsed_data = parse_en(text,tagset="universal",tokenize=True,lemmata=True)
            parsed_data = split_en(parsed_data)
        elif self.main_language == "fr":
            parsed_data = parse_fr(text,tagset="universal",tokenize=True,lemmata=True)
            parsed_data = split_fr(parsed_data)
        elif self.main_language == "it":
            parsed_data = parse_it(text, tagset="universal", tokenize=True, lemmata=True)
            parsed_data = split_it(parsed_data)
        elif self.main_language == "de":
            parsed_data = parse_de(text, tagset="universal", tokenize=True, lemmata=True)
            parsed_data = split_de(parsed_data)



        output = []
        start_idx_looking = 0
        for sentence in parsed_data:
            #if self.language == "en":
            #    tokens = nltk.word_tokenize(sentence, language=self.language)
            #    tokens_pos = nltk.pos_tag(tokens)
            #elif self.language == "de" or self.language == "als":
            tokens_pos = self.transform_parsing(sentence)

            #tokens = nltk.word_tokenize(sentence, language=self.full_name_language)
            #tokens_pos = nltk.pos_tag(tokens, lang=self.full_name_language)
            #tokens_pos = tt.tag(sentence)
            end = -1
            link = None
            ner_class = "O"
            sequence = []
            for position, (token, POS) in enumerate(tokens_pos):
                if regex.match(r"\s+", token):
                    continue
                if not token.strip():
                    continue
                if token in ["''", "``", "„", "“" ]:
                    continue
                token_idx = text.find(token, start_idx_looking)
                start_idx_looking = token_idx + 1
                if token_idx < end:
                    if ner_class == "O":
                        sequence.append((token, POS, ner_class, link, False))
                    else:
                        sequence.append((token, POS, "I" + "-" + ner_class, link, False))
                else:
                    ner_class, lc, link, start, end = self.get_ner_class(token_idx)
                    if ner_class != "O":
                        sequence.append((token, POS, "B-" + ner_class, link, lc))
                    else:
                        ner_class = "O"
                        sequence.append((token, POS, ner_class, None, False))
            if len(sequence) <= 200 or len(sequence)<2:
                output.append(sequence)

        return output


    def build_dataset(self, sequences):
        mapping = dict()
        trie = Trie()
        result = ""
        if self.dataset_method in ["wp2", "wp3"]:
            mapping, trie = self._add_all_alternative_titles()
            self.mapping = mapping
            self.trie = trie
        for sentence in sequences:
            at_least_one_entity = False
            link_inferring = False
            to_infer_token = []
            to_infer_POS = []
            to_infer_position = []
            output = ""
            next_BIO_tag = ""
            keep_sentence = True
            next_should_be_PER = False
            to_print = False
            counter = 0
            for position, (token, POS, ner_class, link, lc) in enumerate(sentence):
                #if (token, POS, ner_class, link, lc) == ('The', 'DT', 'O', None, False):
                #    counter += 1
                #    to_print=True
                #Some link must be unlinked such as "List of ...":
                if ner_class != "O":
                    ner_class = self._unlinked(token, link, ner_class)


                if self.dataset_method not in ["wpb", "wp0"] and ner_class == "O" and (is_capitalized(token) or link_inferring):
                    #if to_print:
                    #    print(1, token,ner_class,  is_capitalized(token), link_inferring)
                    to_infer_token.append(token)
                    to_infer_POS.append(POS)
                    to_infer_position.append(position)
                    tmp_output, tmp_at_least_one_entity, tmp_keep_sentence, to_infer_token, to_infer_POS, to_infer_position, link_inferring, next_should_be_PER = self._infer_tokens(trie, to_infer_token, to_infer_POS, to_infer_position, mapping, next_should_be_PER)
                    output  += tmp_output
                    at_least_one_entity |= tmp_at_least_one_entity
                    keep_sentence &= tmp_keep_sentence
                else:
                    for t,p, po in zip(to_infer_token, to_infer_POS, to_infer_position):
                        tmp_keep_sentence, next_should_be_PER = self.sentence_selection(t, 'O', 0,po ,
                                                                                        None, next_should_be_PER)
                        keep_sentence &= tmp_keep_sentence
                        line, next_BIO_tag, entity = self._apply_wikiner_rules(t, p, 'O', None,
                                                                               next_BIO_tag)
                        at_least_one_entity |= entity
                        output += line
                    to_infer_token = []
                    to_infer_POS = []
                    to_infer_position = []
                    #keep_sentence &= self.sentence_selection(token, ner_class, lc, position, link)
                    #if to_print:
                    #    print(2, token)
                    tmp_keep_sentence, next_should_be_PER = self.sentence_selection(token, ner_class, lc, position, link, next_should_be_PER)
                    keep_sentence &= tmp_keep_sentence
                    line, next_BIO_tag, entity = self._apply_wikiner_rules(token, POS, ner_class, link, next_BIO_tag)
                    at_least_one_entity |= entity
                    output += line

                #print(token, next_should_be_PER, keep_sentence)
                if self.dataset_method != "wpb" and not keep_sentence:
                    at_least_one_entity = False
                    break


            if at_least_one_entity and not next_should_be_PER:
                result += output + " \n"

            #if counter == 2:
            #    return result
            #print("")
        return result


    def _infer_tokens(self, trie, to_infer_token, to_infer_POS, to_infer_position, mapping, next_should_be_PER,force_infer=False):
        output = ""
        at_least_one_entity = False
        keep_sentence = True
        link_inferring = True
        phrase = " ".join(to_infer_token)
        trie_prefixes = trie.start_with_prefix(phrase)
        if len(trie_prefixes) == 0 and not force_infer:
            if len(to_infer_token) == 1:
                tmp_output, tmp_at_least_one_entity, tmp_keep_sentence, to_infer_token, to_infer_POS, to_infer_position, link_inferring, next_should_be_PER = self._infer_tokens(trie,
                                                                                                        to_infer_token,
                                                                                                        to_infer_POS,
                                                                                                        to_infer_position,
                                                                                                        mapping,
                                                                                                        next_should_be_PER,
                                                                                                        force_infer=True
                                                                                                        )

            else:
                tmp_output, tmp_at_least_one_entity, tmp_keep_sentence, _, _, _, _, next_should_be_PER = self._infer_tokens(trie, to_infer_token[:-1], to_infer_POS[:-1], to_infer_position[:-1], mapping,next_should_be_PER, force_infer=True)
                to_infer_token = to_infer_token[-1:]
                to_infer_POS = to_infer_POS[-1:]
                to_infer_position = to_infer_position[-1:]
                link_inferring = True

            output += tmp_output
            at_least_one_entity |= tmp_at_least_one_entity
            keep_sentence &= tmp_keep_sentence
        elif len(trie_prefixes) == 1 or force_infer:
            #if phrase in trie_prefixes:
            new_link = mapping.get(phrase, None)
            if new_link not in self.wp_to_ner_by_title or self.wp_to_ner_by_title[new_link]["ner"] == "DISAM" or self.wp_to_ner_by_title[new_link]["ner"] == "TITLE":
                # Nothing was inferred. shift the windows to the right until the first token is capitalized.
                # in the meantime log the token we output of the window
                for i, (token_to_infer, POS_to_infer, position_to_infer) in enumerate(
                        zip(to_infer_token, to_infer_POS, to_infer_position)):
                    tmp_keep_sentence, next_should_be_PER = self.sentence_selection(token_to_infer, "O", False,position_to_infer, "", next_should_be_PER)
                    keep_sentence &= tmp_keep_sentence
                    #keep_sentence &= self.sentence_selection(token_to_infer, "O", False, position_to_infer, "")
                    output += token_to_infer + " " + POS_to_infer + " " + "O" + " " + "\n"
            else:
                elem = self.wp_to_ner_by_title[new_link]
                ner_class_inferred = elem['ner']
                lowercase = elem['lc'] == 1
                BIO = "B"
                next_BIO_tag = ""
                for i, (token_to_infer, POS_to_infer, position_to_infer) in enumerate(
                        zip(to_infer_token, to_infer_POS, to_infer_position)):
                    tmp_keep_sentence, next_should_be_PER = self.sentence_selection(token_to_infer, ner_class_inferred,lowercase, position_to_infer, new_link, next_should_be_PER)
                    keep_sentence &= tmp_keep_sentence
                    #keep_sentence &= self.sentence_selection(token_to_infer, ner_class_inferred,lowercase, position_to_infer, new_link)
                    line, next_BIO_tag, entity = self._apply_wikiner_rules(token_to_infer,
                                                                           POS_to_infer,
                                                                           BIO + "-" + ner_class_inferred,
                                                                           new_link + "&&INF ",
                                                                           next_BIO_tag)
                    output += line
                    at_least_one_entity |= entity
                    BIO = "I"

            link_inferring = False
            to_infer_token = []
            to_infer_POS = []
            to_infer_position = []
        return output, at_least_one_entity, keep_sentence, to_infer_token, to_infer_POS, to_infer_position, link_inferring, next_should_be_PER


    def _apply_wikiner_rules(self, token, POS, tag, link, next_BIO_tag):
        at_least_one_entity = False
        BIO_tag, ner_type = split_tag(tag)
        if self.dataset_method == "wpb":
            output = token + " " + POS + " " + tag + " " + (link if link else "") + " \n"
        else:
            if next_BIO_tag:
                if next_BIO_tag == "NATIONALITY":
                    if ner_type == "MISC":
                        BIO_tag = "I"
                    else:
                        BIO_tag = "B"
                else:
                    BIO_tag = next_BIO_tag
            next_BIO_tag = None
            # if London, England => in conll it is labelled as London B-LOC , O England B-LOC
            if (token == ',' and ner_type == 'LOC') or (token in string.punctuation and ner_type != "O"):
                ner_type = "O"
                output = token + " " + POS + " " + ner_type + " " + link + " \n"
                next_BIO_tag = 'B'
            #elif POS in ["JJ","RB", "ADJA"] and (ner_type == "LOC" or self.is_nationality(token, POS)):
            elif self.is_nationality(token, POS):
                if not link:
                    link = ""
                    BIO_tag = "B"
                link += "&&NATIONALITY"
                at_least_one_entity = True
                ner_type = "MISC"
                output = token + " " + POS + " " + BIO_tag + "-" + ner_type + " " + link + " \n"
                next_BIO_tag = "NATIONALITY"
            elif token in self.PERSONAL_TITLES and ner_type == "PER" and BIO_tag == "B":
                ner_type = "O"
                output = token + " " + POS + " " + ner_type + "\n"
                next_BIO_tag = "B"
            else:
                if ner_type == "O":
                    output = token + " " + POS + " " + tag + " " + (link if link else "") + " \n"
                else:
                    output = token + " " + POS + " " + BIO_tag + "-" + ner_type + " " + (link if link else "") + " \n"

        if ner_type != "O":
            at_least_one_entity = True
        return output, next_BIO_tag, at_least_one_entity

    def is_nationality(self, token, POS):
        if self.main_language in constants.NATIONALITY_IS_CAP:
            if POS in ["JJ", "RB", "ADJA", "ADJ"]:
                return token[0].isupper() and token in self.NATIONALITIES
        elif self.main_language in constants.NATIONALITY_DO_NOT_NEED_TO_BE_CAP:
            if POS in ["JJ", "RB", "ADJA", "ADJ"]:
                return token in self.NATIONALITIES #or token.capitalize() in self.NATIONALITIES or token.lower() in self.NATIONALITIES
        else:
            raise NotImplemented

    def _add_all_alternative_titles(self):
        mapping = dict()
        trie = Trie()
        #self._add_title_as_alternative(mapping, trie)
        self._add_alternative_titles(self.title, mapping, trie)
        for link in self.out_links:
            self._add_alternative_titles(link, mapping, trie)
        return mapping, trie

    def _add_alternative_titles(self, link, mapping, trie):
        if link in self.alternative_titles:
            alternatives = self.alternative_titles[link]
            for alt in alternatives:
                if alt not in mapping and self._accept_title(alt):
                    trie.add(alt)
                    mapping[alt] = link

    def _add_title_as_alternative(self, mapping,trie):
        if self._accept_title(self.title):
            trie.add(self.title)
            mapping[self.title] = self.title
            self._add_alternative_titles(self.title, mapping, trie)
            #for part in self.title.split():
                #if is_capitalized(part):
                #    trie.add(part)
                #    mapping[part] = self.title

    def _accept_title(self, title):
        if "List of " in title:
            return False
        else:
            return True




    def sentence_selection(self, token, ner_class, lowercase_template, position, link, next_should_be_PER):
        """
         Return True if the token is a valid token in a sentence
         Criterion:
            -Rejected if:
                -  All-lowercase links to entity articles, unless the target is marked Lowercase;
                -  Capitalised text unlinked except if:
                    - Accepted if:
                        - first words of sentences
                        - pronouns (only first person I in English)
                        - dates (e.g. Monday, February, 70 AD)
                        - acronyms
                        - personal titles (e.g. Mr., President)
                        - adjectival forms of names (e.g. nationalities).
                - otherwise accept
         Return: True if accepted, if the next token has to be a PER
        """
        if next_should_be_PER and ner_class != "B-PER":
            return False, False
        if token.islower() and not lowercase_template and ner_class != 'O' and token != link:
            return False, False
        if token[0].isupper():
            if ner_class != 'O':
                return True, False
            if self.is_personal_titles(token) and (ner_class == "O" or "PER" in ner_class):
                return True, True
            if position == 0 and token.lower() in self.SENTENCE_STARTER:
                return True, False
            if token == 'I':
                return True, False
            if is_date(token, self.main_language) or token in self.CALENDAR or token.lower() in self.CALENDAR or token.capitalize() in self.CALENDAR:
                return True, False
            # if all uppercase => acronym : acceptQ18602249
            if token.isupper() and len(token) > 1:
                return True, False
            if token in self.ALLOWED_WITH_CAP:
                return True, False
            return False, False
        return True, False

    def is_personal_titles(self, token):
        if not token[0].isupper():
            return False
        if token in self.PERSONAL_TITLES:
            return True
        if token.upper() in self.PERSONAL_TITLES:
            return True
        if token+"." in self.PERSONAL_TITLES:
            return True
        if token.upper()+"." in self.PERSONAL_TITLES:
            return True
        else:
            return False

    def get_ner_class(self, token_idx):
        if token_idx in self.indexes_to_link:
            ((start, end), link) = self.indexes_to_link[token_idx]
            if link in self.wp_to_ner_by_title and self.wp_to_ner_by_title[link]["ner"] != "DISAM" and self.wp_to_ner_by_title[link]["ner"] != "TITLE":
                elem = self.wp_to_ner_by_title[link]
                return elem['ner'], elem['lc'] == 1, link,  start, end
            elif link.capitalize() in self.wp_to_ner_by_title and self.wp_to_ner_by_title[link.capitalize()]["ner"] != "DISAM" and self.wp_to_ner_by_title[link.capitalize()]["ner"] != "TITLE":
                elem = self.wp_to_ner_by_title[link.capitalize()]
                return elem['ner'], elem['lc'] == 1, link,  start, end
            else:
                return "O", False, link, -1, -1
        else:
            return "O", False, None, -1, -1

    def _unlinked(self, token, link, ner_class):
        if self._accept_title(link):
            return ner_class
        if token == "The":
            return 'O'
        if is_int(token):
            return 'O'
        else:
            return "O"














