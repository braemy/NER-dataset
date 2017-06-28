import string
import sys

import os
import regex

#from pyspark.sql import *
#from pyspark.sql.functions import *
#from pyspark.sql.types import *
sys.path.append("../")
from utils import *
from Trie import Trie
import string
#from spacy.en import English

import nltk
nltk.data.path.append('/home/braemy/nltk_data/')



class Wiki_text(object):
    def __init__(self, row,dataset_method, alternative_titles=None, personal_titles=None, sentence_starter=None, cleaning=False):
        self.text = row['text']
        self.title = row['title']
        if cleaning:
            self.id = row['id']
        self.indexes_to_link = dict()
        self.PERSONAL_TITLES = personal_titles
        self.alternative_titles = alternative_titles
        self.dataset_method = dataset_method
        self.SENTENCE_STARTER = sentence_starter


    @staticmethod
    def is_redirect(text):
        return "#REDIRECT" in text

    def is_redirect_(self):

        return "#REDIRECT" in self.text

    def parse(self,wp_to_ner_by_title, personal_titles,alternative_titles):
        self.PERSONAL_TITLES=personal_titles
        self.alternative_titles = alternative_titles
        self.text = self.apply_regex(self.text)
        subpart = self._map_title(self.text)
        subpart = self._map_link(subpart)
        self.text = subpart


        self.parsed_text = self._parse(subpart,wp_to_ner_by_title=wp_to_ner_by_title)
        self.parsed_text = self.build_dataset(self.parsed_text, wp_to_ner_by_title)
        return self.parsed_text

    def parse_spark(self, wp_to_ner_by_title):
        parsed_text = ""
        text = self._map_title(self.text)
        text = self._map_link(text)
        sequence = self._parse(text, wp_to_ner_by_title=wp_to_ner_by_title)
        self.text = self.build_dataset(sequence, wp_to_ner_by_title)
        return Row(title=self.title, text=self.text)

    def clean(self):
        try:
            cleaned_text = self.apply_regex(self.text)
            return Row(title=self.title, text = cleaned_text, back=0)

        except:
            return Row(title=self.title, text = self.text, back=1)

    def _print_parsed_text(self):
        for sentence in self.parsed_text:
            for tuple in sentence:
                print(tuple)
            print("\n")



    def _replace_wikt(self,text):
        reg = r"\[\[wikt:([\S\s]*?)\|([\S\s]*?)\]\]"
        result = regex.finditer(reg, text)
        output = text
        for r in result:
            output = output.replace(text[r.start():r.end()], r.group(2), 1)
        return output



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

    def _print_mapping(self):
        for _, ((start,end), link) in self.indexes_to_link.items():
            print(self.text[start:end], (start, end), '-->', link)



    def _map_title(self, text):
        reg = r"\'\'\'([\S\s]*?)\'\'\'"
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


    def _parse(self,text, wp_to_ner_by_title=None):
        parsed_data = nltk.sent_tokenize(text)
        output = []
        start_idx_looking = 0
        for sentence in parsed_data:
            tokens = nltk.word_tokenize(sentence)
            tokens_pos = nltk.pos_tag(tokens)
            end = -1
            link = None
            ner_class = "O"
            sequence = []
            for position, (token, POS) in enumerate(tokens_pos):
                if regex.match(r"\s+", token):
                    continue
                token_idx = text.find(token, start_idx_looking)
                start_idx_looking = token_idx + 1
                if token_idx < end:
                    if ner_class == "O":
                        sequence.append((token, POS, ner_class, link, False))
                    else:
                        sequence.append((token, POS, "I" + "-" + ner_class, link, False))
                else:
                    ner_class, lc, link, start, end = self.get_ner_class(token_idx, wp_to_ner_by_title)
                    if ner_class != "O":
                        sequence.append((token, POS, "B-" + ner_class, link, lc))
                    else:
                        ner_class = "O"
                        sequence.append((token, POS, ner_class, None, False))
            output.append(sequence)

        return output


    def build_dataset(self, sequences, wp_to_ner_by_title):
        mapping = dict()
        trie = Trie()
        result = ""
        if self.dataset_method not in ["wpb", "wp0"]:
            self._add_alternative_titles(self.title, mapping, trie)
        for sentence in sequences:
            at_least_one_entity = False
            link_inferring = False
            to_infer_token = []
            to_infer_POS = []
            to_infer_position = []
            output = ""
            next_BIO_tag = ""
            keep_sentence = True
            for position, (token, POS, ner_class, link, lc) in enumerate(sentence):
                if self.dataset_method not in ["wpb", "wp0"]:
                    #add alternative title at the already existing ones
                    self._add_alternative_titles(link, mapping, trie)  # TODO ADD CONDITION SEE page 72 of WIKINER

                if self.dataset_method not in ["wpb", "wp0"] and ner_class == "O" and (self._is_capitalized(token) or link_inferring):
                    to_infer_token.append(token)
                    to_infer_POS.append(POS)
                    to_infer_position.append(position)
                    tmp_output, tmp_at_least_one_entity, tmp_keep_sentence, to_infer_token, to_infer_POS, to_infer_position, link_inferring = self._infer_tokens(trie, to_infer_token, to_infer_POS, to_infer_position, wp_to_ner_by_title, mapping)
                    output  += tmp_output
                    at_least_one_entity |= tmp_at_least_one_entity
                    keep_sentence &= tmp_keep_sentence
                else:
                    keep_sentence &= self.sentence_selection(token, ner_class, lc, position)
                    line, next_BIO_tag, entity = self._apply_wikiner_rules(token, POS, ner_class, link, next_BIO_tag)
                    at_least_one_entity |= entity
                    output += line

                if self.dataset_method != "wpb" and not keep_sentence:
                    at_least_one_entity = False
                    break

            if at_least_one_entity:
                #print(output)
                result += output + " \n"

        return result


    def _infer_tokens(self, trie, to_infer_token, to_infer_POS, to_infer_position, wp_to_ner_by_title, mapping, force_infer=False):
        output = ""
        at_least_one_entity = False
        keep_sentence = True
        link_inferring = True
        phrase = " ".join(to_infer_token)
        trie_prefixes = trie.start_with_prefix(phrase)
        if "Various" in to_infer_token:
            a=1
        if len(trie_prefixes) == 0 and not force_infer:
            if len(to_infer_token) == 1:
                tmp_output, tmp_at_least_one_entity, tmp_keep_sentence, to_infer_token, to_infer_POS, to_infer_position, link_inferring = self._infer_tokens(trie,
                                                                                                        to_infer_token,
                                                                                                        to_infer_POS,
                                                                                                        to_infer_position,
                                                                                                        wp_to_ner_by_title,
                                                                                                        mapping,
                                                                                                        force_infer=True)

            else:
                tmp_output, tmp_at_least_one_entity, tmp_keep_sentence, _, _, _, _ = self._infer_tokens(trie, to_infer_token[:-1], to_infer_POS[:-1], to_infer_position[:-1], wp_to_ner_by_title, mapping, force_infer=True)
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
            if not new_link in wp_to_ner_by_title:
                # Nothing was inferred. shift the windows to the right until the first token is capitalized.
                # in the meantime log the token we output of the window
                for i, (token_to_infer, POS_to_infer, position_to_infer) in enumerate(
                        zip(to_infer_token, to_infer_POS, to_infer_position)):
                    keep_sentence &= self.sentence_selection(token_to_infer, "O", False,
                                                             position_to_infer)
                    output += token_to_infer + " " + POS_to_infer + " " + "O" + " " + "\n"
            else:
                elem = wp_to_ner_by_title[new_link]
                ner_class_inferred = elem['ner']
                lowercase = elem['lc'] == 1
                BIO = "B"
                next_BIO_tag = ""
                for i, (token_to_infer, POS_to_infer, position_to_infer) in enumerate(
                        zip(to_infer_token, to_infer_POS, to_infer_position)):
                    keep_sentence &= self.sentence_selection(token_to_infer, ner_class_inferred,
                                                             lowercase, position_to_infer)
                    line, next_BIO_tag, entity = self._apply_wikiner_rules(token_to_infer,
                                                                           POS_to_infer,
                                                                           BIO + "-" + ner_class_inferred,
                                                                           new_link + "&&INF ",
                                                                           next_BIO_tag)
                    output += line
                    at_least_one_entity |= entity
                    BIO = "I"
                    # TODO add move the window to the right and continue with the remainging part...

            link_inferring = False
            to_infer_token = []
            to_infer_POS = []
            to_infer_position = []
        return output, at_least_one_entity, keep_sentence, to_infer_token, to_infer_POS, to_infer_position, link_inferring


    def _apply_wikiner_rules(self, token, POS, tag, link, next_BIO_tag):
        at_least_one_entity = False
        BIO_tag, ner_type = self._split_tag(tag)
        if self.dataset_method == "wpb":
            output = token + " " + POS + " " + tag + " " + (link if link else "") + " \n"
        else:
            if next_BIO_tag:
                BIO_tag = next_BIO_tag
            next_BIO_tag = None
            # if London, England => in conll it is labelled as London B-LOC , O England B-LOC
            if (token == ',' and ner_type == 'LOC'):
                ner_type = "O"
                output = token + " " + POS + " " + ner_type + " " + link + " \n"
                next_BIO_tag = 'B'
            elif POS == "JJ" and ner_type == "LOC":
                at_least_one_entity = True
                ner_type = "MISC"
                output = token + " " + POS + " " + BIO_tag + "-" + ner_type + " " + link + " \n"
                next_BIO_tag = "B"
            elif token in self.PERSONAL_TITLES:
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

    def _add_alternative_titles(self, link, mapping, trie):
        if link in self.alternative_titles:
            alternatives = self.alternative_titles[link]
            if link == "Mikhail Bakunin":
                a=1
            for alt in alternatives:
                if alt not in mapping:
                    trie.add(alt)
                    mapping[alt] = link  # TODO ADD CONDITION SEE page 72 of WIKINER
        return mapping

    @staticmethod
    def _split_tag(ner_class):
        if ner_class == "O":
            return None, "O"
        return ner_class.split("-")

    def _infer_link(self, tokens, mapping, ):
        for i in range(len(tokens)):
            for j in range(len(tokens), 0, -1):
                try:
                    return mapping[" ".join(tokens[i:j])], i,j
                except KeyError:
                    continue
        return None, 0, len(tokens)



    @staticmethod
    def _is_capitalized(token):
        return token[0].isupper()

    def sentence_selection(self, token, ner_class, lowercase_template, position):
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
         Return: True if accepted
        """
        if token.islower() and not lowercase_template and ner_class != 'O':
            return False
        if token[0].isupper():
            if ner_class != 'O':
                return True
            if position == 0 and token.title() in self.SENTENCE_STARTER:
                return True
            if token == 'I':
                return True
            if is_date(token):
                return True
            if self.is_personal_titles(token):
                return True
            # if all uppercase => acronym : accept
            if token.isupper():
                return True
            # TODO nationalities
            return False
        return True

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



    def get_ner_class(self, token_idx, table):
        if token_idx in self.indexes_to_link:
            ((start, end), link) = self.indexes_to_link[token_idx]
            try:
                elem = table[link]
                return elem['ner'], elem['lc'] == 1, link,  start, end
            except KeyError:
                try:
                    elem = table[link.capitalize()]
                    return elem['ner'], elem['lc'] == 1, link,  start, end
                except KeyError:
                    return "O", False, link, -1, -1
        else:
            return "O", False, None, -1, -1


    def apply_regex(self, text):
        text = self._remove_ref(text)
        text = self._remove_list(text)
        text = self._remove_braces(text)
        text = self._remove_comment(text)
        text = self._remove_section(text)
        text = self._remove_categories(text)
        text = self._remove_math(text)
        text = self._remove_table(text)
        text = self._remove_sub(text)
        text = self._remove_image(text)
        text = self._remove_blockquote(text)
        text = self._remove_sup(text)
        text = self._remove_small(text)
        text = self._remove_big(text)
        text = self._remove_div(text)
        text = self._remove_parenthesis(text)
        text = self._remove_gallery(text)
        text = self._remove_italic(text)
        text = self._remove_source(text)
        text = self._replace_wikt(text)  # TODO yes or no?

        return text



    def _remove_ref(self,text):
        #ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
        ref_regex = r"<ref[^\/]*?>([^<]|(?R))*?<\/ref>|<ref[\S\s]*?\/>"
        text = regex.sub(ref_regex, "", text)
        #remove everything that is below the section == References ==
        text = text[:text.find("== References ==")]
        return text[:text.find("==References==")]
    def _remove_braces(self,text):
        braces_regex = r"\{\{([^\{\}]|(?R))*\}\}"
        return regex.sub(braces_regex, "", text)

    def _remove_comment(self,text):
        comment_regex = r"<!--[\S\s]*?-->"
        return regex.sub(comment_regex, "", text)

    def _remove_list(self,text):
        list_regex = r"^(\*|#|:|;)+.*?\n"
        return regex.sub(list_regex, "", text, flags=regex.MULTILINE)

    def _remove_section(self,text):
        section_regex = r"=+.*?=+"
        return regex.sub(section_regex, "", text)

    def _remove_categories(self,text):
        categories_regex = r"\[\[:*Category:.*?\]\]"
        return regex.sub(categories_regex, "", text)

    def _remove_math(self,text):
        math_regex = r"<math[^\/]*?>[\S\s]*?<\/math>"
        return regex.sub(math_regex, "", text)

    def _remove_table(self,text):
        table_regex = r"\{\|[\S\s]*?(\{\|[\S\s]*?\|\}[\S\s]*?)*?\|\}"#\{\|([\S\s]|(?R))*?\|\}
        return regex.sub(table_regex, "", text)

    def _remove_sub(self,text):
        sub_regex = r"<sub[^\/]*?>[\S\s]*?<\/sub>"
        return regex.sub(sub_regex, "", text)
    def _remove_image(self,text):
        image_regex = r"\[\[(Image|File):[\S\s]*?(\[\[[\S\s]*?\]\][\S\s]*?)*?\]\]"  # TODO keep caption
        return regex.sub(image_regex, "", text)

    def _remove_blockquote(self,text):
        blockquote_regex = r"<blockquote[^\/]*?>[\S\s]*?<\/blockquote>"
        return regex.sub(blockquote_regex, "", text)

    def _remove_sup(self,text):
        sup_regex = r"<sup[^\/]*?>[\S\s]*?<\/sup>"
        return regex.sub(sup_regex, "", text)
    def _remove_small(self,text):
        small_regex = r"<small[^\/]*?>[\S\s]*?<\/small>"
        return regex.sub(small_regex, "", text)
    def _remove_big(self,text):
        big_regex = r"<big[^\/]*?>[\S\s]*?<\/big>"
        return regex.sub(big_regex, "", text)
    def _remove_div(self,text):
        div_regex = r"<div[^\/]*?>[\S\s]*?<\/div>"
        return regex.sub(div_regex, "", text)

    def _remove_parenthesis(self,text):
        empty_parenthesis = r"\(\s*\)"
        return regex.sub(empty_parenthesis, "", text)

    def _remove_gallery(self,text):
        gallery = r"<gallery[^\/]*?>[\S\s]*?<\/gallery>"
        return regex.sub(gallery, "", text)

    def _remove_italic(self,text):
        def replace(o):
            return o.group(1) + o.group(3)
        italic = r"([^']|^)('')([^'])"
        return regex.sub(italic, replace, text, flags=regex.MULTILINE)

    def _remove_source(self,text):
        div_regex = r"<source[^\/]*?>[\S\s]*?<\/source>"
        return regex.sub(div_regex, "", text)




















    def _get_regex(self):
        ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
        ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>" #<ref[^\/]*?>([^<]|(?R))*?<\/ref>
        braces_regex = r"\{\{([^\{\}]*|(?R))*\}\}"     #r"\{\{([^\{\}]|(?R))*\}\}"
        comment_regex = r"<!--[\S\s]*?-->"
        list_regex = r"^(\*|:|;)+.*?\n"
        section_regex = r"=+.*?=+"
        categories_regex = r"\[\[:*Category:.*?\]\]"
        math_regex = r"<math[^\/]*?>[\S\s]*?<\/math>"
        blockquote_regex = r"<blockquote[^\/]*?>[\S\s]*?<\/blockquote>"
        table_regex = r"\{\|[\S\s]*?(\{\|[\S\s]*?\|\}[\S\s]*?)*?\|\}"
        sub_regex = r"<sub[^\/]*?>[\S\s]*?<\/sub>"
        sup_regex = r"<sup[^\/]*?>[\S\s]*?<\/sup>"
        image_regex = r"\[\[(Image|File):[\S\s]*?(\[\[[\S\s]*?\]\][\S\s]*?)*?\]\]"         # TODO keep caption
        small_regex = r"<small[^\/]*?>[\S\s]*?<\/small>"
        big_regex = r"<big[^\/]*?>[\S\s]*?<\/big>"
        div_regex = r"<div[^\/]*?>[\S\s]*?<\/div>"
        empty_parenthesis = r"\(\)"
        gallery = r"<gallery[^\/]*?>[\S\s]*?<\/gallery>"


        return '|'.join([ref_regex,comment_regex,list_regex, section_regex, categories_regex, braces_regex,
                         math_regex,table_regex,sub_regex,image_regex,sup_regex,blockquote_regex, small_regex,
                         div_regex,empty_parenthesis,gallery,big_regex])




    class ToBeInferred(object):
        def __init__(self):
            self.token = []
            self.POS = []
            self.position = []

        def reset(self):
            self.token = []
            self.POS = []
            self.position = []

        def add(self, token, POS, position):
            self.token.append(token)
            self.token.append(POS)
            self.position.append(position)

        def get(self,start_index, end_index):
            return self.token[start_index:end_index],\
                   self.POS[start_index:end_index],\
                   self.position[start_index:end_index]