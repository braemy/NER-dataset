import sys

import os
import re
import regex

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils import *
#from spacy.en import English

import nltk
nltk.data.path.append('/home/braemy/nltk_data/')

from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize

class Wiki_text(object):
    def __init__(self, row):
        self.text = row['text']
        self.title = row['title']
        self.indexes_to_link = dict()



    #def __init__(self, text):
    #    self.text = text
    #    self.indexes_to_link = dict()
    @staticmethod
    def is_redirect(text):
        return "#REDIRECT" in text

    def is_redirect_(self):

        return "#REDIRECT" in self.text

    def parse(self,wp_to_ner_by_title):
        regex_list = self._get_regex()
        self.text = regex.sub(regex_list, "", self.text)
        self._replace_wikt() # TODO yes or no?
        output = self._map_link(self.text)
        output = self._map_title(output)
        self.parsed_text, _ = self._parse(output,wp_to_ner_by_title=wp_to_ner_by_title)


    def parse_spark(self, wp_to_ner_by_title=None):
        parsed_text = ""
        subpart = self._map_title(self.text)
        subpart = self._map_link(subpart)
        subpart = self._title_inference(subpart)
        _, subpart = self._parse(subpart, wp_to_ner_by_title=wp_to_ner_by_title)

        return Row(title=self.title, text=subpart)
        #return Row(title=self.title, text=self.text)

    def clean(self):
        regex_list = self._get_regex()
        self.text = regex.sub(regex_list, "", self.text)
        self._replace_wikt()  # TODO yes or no?
        return Row(title=self.title, text = self.text)

    def _print_parsed_text(self):
        for sentence in self.parsed_text:
            for tuple in sentence:
                print(tuple)
            print("\n")
    def _get_regex(self):
        ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
        braces_regex = r"\{\{([^\{\}]|(?R))*\}\}"
        comment_regex = r"<!--[\S\s]*?-->"
        list_regex = r"\*+.*?\n"
        section_regex = r"=+.*?=+"
        categories_regex = r"\[\[:*Category:.*?\]\]"
        math_regex = r"<math[^\/]*?>[\S\s]*?<\/math>"
        blockquote_regex = r"<blockquote[^\/]*?>[\S\s]*?<\/blockquote>"
        table_regex = r"\{\|[\S\s]*?(\{\|[\S\s]*?\|\}[\S\s]*?)*?\|\}"
        sub_regex = r"<sub[^\/]*?>[\S\s]*?<\/sub>"
        sup_regex = r"<sup[^\/]*?>[\S\s]*?<\/sup>"
        image_regex = R"\[\[(Image|File):[\S\s]*?(\[\[[\S\s]*?\]\][\S\s]*?)*?\]\]"         # TODO keep caption


        return '|'.join([ref_regex,comment_regex,list_regex, section_regex, categories_regex, braces_regex,
                         math_regex,table_regex,sub_regex,image_regex,sup_regex,blockquote_regex])

    def _replace_wikt(self):
        regex = r"\[\[wikt:([\S\s]*?)\|([\S\s]*?)\]\]"
        result = re.finditer(regex, self.text)
        output = self.text
        for r in result:
            output = output.replace(self.text[r.start():r.end()], r.group(2), 1)
        self.text = output



    def _map_link(self, text):
        regex = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
        result = re.finditer(regex, text)
        output = text

        for r in result:
            replace_with = r.group(1) if r.group(3) is None else r.group(3)
            link = r.group(1)
            start = output.find(text[r.start():r.end()])
            self.indexes_to_link[start] = ((start, start + len(replace_with)),link)
            output = output.replace(text[r.start():r.end()], replace_with, 1)
        return output

    def _print_mapping(self):
        for _, ((start,end), link) in self.indexes_to_link.items():
            print(start, end,link)
            print(self.text[start:end], '-->', link)



    def _map_title(self, text):
        regex = r"\'\'\'([\S\s]*?)\'\'\'"
        regex = re.compile(regex)
        result = regex.finditer(text)
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
            self._title_inference(output, r.group(1))
            result = re.finditer(title, text)
            output = text
            previous_index = 0
            for r in result:
                start = output.find(text[r.start():r.end()], previous_index)
                previous_index = start + 1
                self.indexes_to_link[start] = ((start, start + len(title)),title)


    def _parse(self,text, wp_to_ner_by_title=None, wikipedia_to_wikidata = None, wikidata_to_ner=None, wikiTitle_to_id=None):
        #command = "../apache-opennlp-1.7.2/bin/opennlp SentenceDetector models/en-sent.bin < 'Hello my name is John. I live in switzerland.'"
        #instance = opennlp.OpenNLP("/home/braemy/wiki/apache-opennlp-1.7.2", "SentenceDetector", "en-sent.bin")
        #instance = opennlp.OpenNLP("../apache-opennlp-1.7.2", "TokenizerME","en-token.bin")
        #print(os.system(command))
        #tokens = instance.parse("'''Anarchism''' is a [[political philosophy]] that advocates [[self-governance|self-governed]] societies based on voluntary institutions.")

        #tokens = instance.parse("'''Anarchism''' is a **political philosophy** that advocates **self-governed** societies based on voluntary institutions.")
        #print(tokens)
        #if not parser:
        #    parser = English(parser=False)
        parsed_data = sent_tokenize(text)

        #if wp_to_ner_by_title is None:
        #    wp_to_ner_by_title = load_pickle_spark("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")

        #tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
        #sent_tokenize_list = tokenizer.tokenize(text)

        output = []
        sequence_list_str = ""
        start_idx_looking = 0
        for sentence in parsed_data:
             tokens = word_tokenize(sentence)
             end = -1
             link = None
             ner_class = "O"
             number_of_ner_class = 0
             sequence = []
             sentence_str = ""
             for token in tokens:
                 if re.match(r"\s+", token):
                     continue
                 token_idx = text.find(token, start_idx_looking)
                 start_idx_looking = token_idx + 1
                 if token_idx < end:
                     if ner_class == "O":
                         sequence.append((token, link, ner_class))
                         sentence_str += token  +" X "+ ner_class +" " +link+ " \n"
                     else:
                         sequence.append((token, link, "I-" +ner_class))
                         sentence_str += token + " X " + "I-" + ner_class + " " + link + " \n"
                         number_of_ner_class += 1
                 else:
                     try:
                         ((start, end), link) = self.indexes_to_link[token_idx]
                         ner_class = get_ner_class(link, wp_to_ner_by_title)
                         if ner_class:
                             sequence.append((token, link, "B-" + ner_class))
                             sentence_str += token  +" X "+ "B-"+ner_class +" " +link+ " \n"
                             number_of_ner_class += 1
                         else:
                             ner_class = "O"
                             sequence.append((token, link, ner_class))
                             sentence_str += token +" X " + ner_class +" " +link+ " \n"

                     except:
                         ner_class = "O"
                         sequence.append((token, None, ner_class))
                         sentence_str += token + " X " + ner_class +" None \n"

             if float(number_of_ner_class)/float(len(tokens)) > 0.3 :
                 output.append(sequence)
                 sequence_list_str += sentence_str +  " \n"

        return output, sequence_list_str




"""
    def _remove_ref(self):
        ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
        self.text = re.sub(ref_regex, "", self.text)

    def _remove_braces(self):
        braces_regex = r"\{\{[\S\s]*?(\{\{[\S\s]*?\}\}[\S\s]*?)*?\}\}"
        self.text = re.sub(braces_regex, "", self.text)

    def _remove_comment(self):
        comment_regex = r"<!--[\S\s]*?-->"
        self.text = re.sub(comment_regex, "", self.text)

    def _remove_list(self):
        list_regex = r"\*+.*?\n"
        self.text = re.sub(list_regex, "", self.text)

    def _remove_section(self):
        section_regex = r"=+.*?=+"
        self.text = re.sub(section_regex, "", self.text)

    def _remove_categories(self):
        categories_regex = r"\[\[:*Category:.*?\]\]"
        self.text = re.sub(categories_regex, "", self.text)

    def _remove_math(self):
        math_regex = r"<math[^\/]*?>[\S\s]*?<\/math>"
        self.text = re.sub(math_regex, "", self.text)

    def _remove_table(self):
        table_regex = r"\{\|[\S\s]*?(\{\|[\S\s]*?\|\}[\S\s]*?)*?\|\}"
        self.text = re.sub(table_regex, "", self.text)

    def _remove_sub(self):
        sub_regex = r"<sub[^\/]*?>[\S\s]*?<\/sub>"
        self.text = re.sub(sub_regex, "", self.text)
    def _remove_image(self):
        image_regex = R"\[\[(Image|File):[\S\s]*?(\[\[[\S\s]*?\]\][\S\s]*?)*?\]\]"
        self.text = re.sub(image_regex, "", self.text)
        """