import string
import sys

import os
import regex

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
sys.path.append("../")
from utils import *
#from spacy.en import English

import nltk
nltk.data.path.append('/home/braemy/nltk_data/')



class Wiki_text(object):
    def __init__(self, row, personal_titles=None, cleaning=False):
        self.text = row['text']
        self.title = row['title']
        if cleaning:
            self.id = row['id']
        self.indexes_to_link = dict()
        self.PERSONAL_TITLES = personal_titles

    #def __init__(self, text):
    #    self.text = text
    #    self.indexes_to_link = dict()
    @staticmethod
    def is_redirect(text):
        return "#REDIRECT" in text

    def is_redirect_(self):

        return "#REDIRECT" in self.text

    def parse(self,wp_to_ner_by_title, personal_titles):
        self.PERSONAL_TITLES=personal_titles
        self.text = self.apply_regex(self.text)
        subpart = self._map_title(self.text)
        subpart = self._map_link(subpart)
        self.text = subpart
        #subpart = self._title_inference(subpart)
        self.parsed_text, _ = self._parse(subpart,wp_to_ner_by_title=wp_to_ner_by_title)


    def parse_spark(self, wp_to_ner_by_title=None):
        parsed_text = ""
        subpart = self._map_title(self.text)
        subpart = self._map_link(subpart)
        #subpart = self._title_inference(subpart)
        _, subpart = self._parse(subpart, wp_to_ner_by_title=wp_to_ner_by_title)

        return Row(title=self.title, text=subpart)
        #return Row(title=self.title, text=self.text)

    def clean(self):
        #regex_list = self._get_regex()
        #self.text = regex.sub(regex_list, "", self.text, flags=regex.MULTILINE)
        cleaned_text = ""
        #for paragraph in self.text.split("\n\n"):
        #    paragraph = self.apply_regex(paragraph)
        #    cleaned_text += paragraph + " \n\n"
        try:
            cleaned_text = self.apply_regex(self.text)
            return Row(title=self.title, text = cleaned_text)

        except:
            print("FUCKING BACKTRACKING")
            print(self.id)
            return Row(title=self.title, text = "FUCKING BACKTRACKING")

    def _print_parsed_text(self):
        for sentence in self.parsed_text:
            for tuple in sentence:
                print(tuple)
            print("\n")

    def _get_regex(self):
        ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
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

    def _title_inference(self,text):

        for title in self.title_list:
            reg = regex.compile(regex.escape(title))
            result = reg.finditer(text)
            output = text
            previous_index = 0
            for r in result:
                start = output.find(text[r.start():r.end()], previous_index)
                previous_index = start + 1
                if start not in self.indexes_to_link:
                    self.indexes_to_link[start] = ((start, start + len(title)),title)
        return text


    def _parse(self,text, wp_to_ner_by_title=None, wikipedia_to_wikidata = None, wikidata_to_ner=None, wikiTitle_to_id=None):
        if self.PERSONAL_TITLES is None:
            self.PERSONAL_TITLES = load_personal_titles()
        parsed_data = nltk.sent_tokenize(text)
        output = []
        sequence_list_str = ""
        start_idx_looking = 0
        for sentence in parsed_data:
            tokens = nltk.word_tokenize(sentence)
            tokens_pos = nltk.pos_tag(tokens)
            end = -1
            link = None
            ner_class = "O"
            number_of_ner_class = 0
            sequence = []
            sentence_str = ""
            at_least_one_entity = False
            BIO_tag = "I"
            for position, (token, POS) in enumerate(tokens_pos):
                if regex.match(r"\s+", token):
                    continue
                token_idx = text.find(token, start_idx_looking)
                start_idx_looking = token_idx + 1
                if token_idx < end:
                    if ner_class == "O":
                        sequence.append((token, link, ner_class))
                        sentence_str += token  +" "+ POS +" "+ ner_class +" " +link+ " \n"
                    else:
                        # if London, England => in conll it is labelled as London B-LOC , O England B-LOC
                        if (token == ',' and ner_class == 'LOC'):
                            sequence.append((token, link, ner_class))
                            sentence_str += token  +" "+ POS +" "+ "O" +" " +link+ " \n"
                            BIO_tag = 'B'

                        else:
                            sequence.append((token, link, BIO_tag+"-" +ner_class))
                            sentence_str += token  +" "+ POS +" "+ BIO_tag+"-" + ner_class + " " + link + " \n"
                            number_of_ner_class += 1
                            BIO_tag = "I"
                else:
                    ner_class, lc, link, start, end = self.get_ner_class(token_idx, wp_to_ner_by_title)
                    if self.sentence_selection(token, ner_class, lc, position):
                        if ner_class != "O":
                            if token in self.PERSONAL_TITLES:
                                sequence.append((token, None, ner_class))
                                sentence_str += token  +" "+ POS +" "+ "O" + "\n"
                                BIO_tag = "B"
                            else:
                                at_least_one_entity = True
                                # if class is LOC and POS is adj => most probably a nationality => MISC
                                if POS == "JJ" and ner_class == "LOC":
                                    ner_class = "MISC"

                                sequence.append((token, link, "B-" + ner_class))
                                sentence_str += token  +" "+ POS +" "+ "B-"+ner_class +" " +link+ " ||"+ self.title + " \n"
                                number_of_ner_class += 1
                        else:
                            ner_class = "O"
                            sequence.append((token, None, ner_class))
                            sentence_str += token  +" "+ POS +" "+ ner_class + "\n"

                    else:
                        at_least_one_entity = False
                        break


                    #except:
                    #    if self.upper_case_but_not_entity(token, position):
                    #        at_least_one_entity = False
                    #        break
                    #    ner_class = "O"
                    #    sequence.append((token, None, ner_class))
                    #    sentence_str += token + " X " + ner_class + "\n"



            #if float(number_of_ner_class)/float(len(tokens)) > 0 :
            if at_least_one_entity:
                output.append(sequence)
                sequence_list_str += sentence_str +  " \n"

        return output, sequence_list_str

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
            if position == 0:
                return True
            if token == 'I':
                return True
            if is_date(token):
                return True
            if token in self.PERSONAL_TITLES:
                return True
            # if all uppercase => acronym : accept
            if token.isupper():
                return True
            # TODO nationalities
            return False
        return True


    def upper_case_but_not_entity(self, token, position):
        """
        a work with capitalize letter is a valid non entity if:

        :param token:
        :return: return True if it is a capitalize token that is not accepted by one of the rule
        #"""
        if token.islower():
            return False
        if token in string.punctuation:
            return False
        if position == 0:
            return False
        if token == 'I':
            return False
        if is_date(token):
            return False
        if token in self.PERSONAL_TITLES:
            return False
        # if all uppercase => acronym : accept
        if token.isupper():
            return False
        return True

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
        ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
        text = regex.sub(ref_regex, "", self.text)
        #remove everything that is below the section == References ==
        text = self.text[:text.find("== References ==")]
        return self.text[:text.find("==References==")]
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
        table_regex = r"\{\|[\S\s]*?(\{\|[\S\s]*?\|\}[\S\s]*?)*?\|\}"
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

