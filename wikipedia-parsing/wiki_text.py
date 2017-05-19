import sys

import os
import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import spacy
from utils import *
from spacy.en import English



class Wiki_text(object):
    def __init__(self, row, load_parser=True):
        self.text = row['text']
        self.title = row['title']
        self.id = row['id']
        self.indexes_to_link = dict()
        if load_parser:
            self.parser = English()


    #def __init__(self, text):
    #    self.text = text
    #    self.indexes_to_link = dict()
    @staticmethod
    def is_redirect(text):
        return "#REDIRECT" in text

    def is_redirect_(self):
        return "#REDIRECT" in self.text

    def parse(self, parser,wp_to_ner_by_title, wikipedia_to_wikidata=None, wikidata_to_ner=None, wikiTitle_to_id=None):
        regex = self._get_regex()
        self.text = re.sub(regex, "", self.text)
        self._replace_wikt() # TODO yes or no?

        self._map_title()
        self._map_link()
        self._parse(parser,wp_to_ner_by_title=wp_to_ner_by_title, wikipedia_to_wikidata=wikipedia_to_wikidata, wikidata_to_ner=wikidata_to_ner, wikiTitle_to_id=wikiTitle_to_id)
        print(len(self.parsed_text))
    def parse_spark(self, wp_to_ner_by_title=None, wikipedia_to_wikidata=None, wikidata_to_ner=None, wikiTitle_to_id=None):
        regex = self._get_regex()
        self.text = re.sub(regex, "", self.text)
        self._replace_wikt() # TODO yes or no?

        self._map_title()
        self._map_link()
        self._parse(wp_to_ner_by_title=wp_to_ner_by_title, wikipedia_to_wikidata=wikipedia_to_wikidata, wikidata_to_ner=wikidata_to_ner, wikiTitle_to_id=wikiTitle_to_id)
        return Row(title=self.title, text=self.parsed_text_str)

    def _print_parsed_text(self):
        for sentence in self.parsed_text:
            for tuple in sentence:
                print(tuple)
            print("\n")
    def _get_regex(self):
        ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
        braces_regex = r"\{\{[\S\s]*?(\{\{[\S\s]*?\}\}[\S\s]*?)*?\}\}"
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


        return '|'.join([ref_regex,braces_regex,comment_regex,list_regex, section_regex, categories_regex,
                         math_regex,table_regex,sub_regex,image_regex,sup_regex,blockquote_regex])

    def _replace_wikt(self):
        regex = r"\[\[wikt:([\S\s]*?)\|([\S\s]*?)\]\]"
        result = re.finditer(regex, self.text)
        output = self.text
        for r in result:
            output = output.replace(self.text[r.start():r.end()], r.group(2), 1)
        self.text = output



    def _map_link(self):
        regex = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
        result = re.finditer(regex, self.text)
        output = self.text

        for r in result:
            replace_with = r.group(1) if r.group(3) is None else r.group(3)
            link = r.group(1)
            start = output.find(self.text[r.start():r.end()])
            self.indexes_to_link[start] = ((start, start + len(replace_with)),link)
            output = output.replace(self.text[r.start():r.end()], replace_with, 1)
        self.text = output

    def _print_mapping(self):
        for (start, end), link in self.indexes_to_link.items():
            print(self.text[start:end], '-->', link)



    def _map_title(self):
        regex = r"\'\'\'([\S\s]*?)\'\'\'"
        result = re.finditer(regex, self.text)
        output = self.text

        for r in result:
            replace_with = r.group(1)
            link = r.group(1)
            start = output.find(self.text[r.start():r.end()])
            self.indexes_to_link[start] = ((start, start + len(replace_with)),link)
            output = output.replace(self.text[r.start():r.end()], replace_with, 1)
        self.text = output

    def _parse(self, parser=None, wp_to_ner_by_title=None, wikipedia_to_wikidata = None, wikidata_to_ner=None, wikiTitle_to_id=None):
        #command = "../apache-opennlp-1.7.2/bin/opennlp SentenceDetector models/en-sent.bin < 'Hello my name is John. I live in switzerland.'"
        #instance = opennlp.OpenNLP("/home/braemy/wiki/apache-opennlp-1.7.2", "SentenceDetector", "en-sent.bin")
        #instance = opennlp.OpenNLP("../apache-opennlp-1.7.2", "TokenizerME","en-token.bin")
        #print(os.system(command))
        #tokens = instance.parse("'''Anarchism''' is a [[political philosophy]] that advocates [[self-governance|self-governed]] societies based on voluntary institutions.")

        #tokens = instance.parse("'''Anarchism''' is a **political philosophy** that advocates **self-governed** societies based on voluntary institutions.")
        #print(tokens)
        if parser:
            parsed_data = parser(self.text)
        else:
            parsed_data = self.parser(self.text)

        #if wikipedia_to_wikidata is None:
            #wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
        #    wikipedia_to_wikidata = load_pickle_spark("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
        #if wikidata_to_ner is None:
            #wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
        #    wikidata_to_ner = load_pickle_spark("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
        #if wikiTitle_to_id is None:
            #wikiTitle_to_id = load_pickle("/dlabdata1/braemy/wikipedia_classification/title_to_id_170.p")
        #    wikiTitle_to_id = load_pickle_spark("/dlabdata1/braemy/wikipedia_classification/title_to_id_170.p")

        if wp_to_ner_by_title is None:
            wp_to_ner_by_title = load_pickle_spark("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")

        output = []
        regex_new_line = r"\n+"
        sequence_list_str = ""
        for sentence in parsed_data.sents:
            end = -1
            link = None
            ner_class = "O"
            at_least_one_link = False
            at_least_one_ner_class = False
            sequence = []
            sentence_str = ""
            for token in sentence:
                if re.match(regex_new_line, token.orth_):
                    continue
                if token.idx < end:
                    if ner_class == "O":
                        sequence.append((token.orth_, link, ner_class))
                        sentence_str += token.orth_  +" X "+ ner_class +" " +link+ " \n"

                    else:
                        sequence.append((token.orth_, link, "I-" +ner_class))
                        sentence_str += token.orth_ + " X " + "I-" + ner_class + " " + link + " \n"

                else:
                    try:
                        ((start, end), link) = self.indexes_to_link[token.idx]
                        at_least_one_link = True
                        #id_link = wikiTitle_to_id[link.capitalize()]
                        #id_link_wikidata = None
                        #if id_link in wikipedia_to_wikidata:
                        #    id_link_wikidata = wikipedia_to_wikidata[id_link]
                        #if id_link_wikidata is not None and id_link_wikidata in wikidata_to_ner:
                        #    ner_class = wikidata_to_ner[id_link_wikidata] # TODO ner shouldn't be an array -> correct that part

                        if link.capitalize() in wp_to_ner_by_title:
                            ner_class = wp_to_ner_by_title[link.capitalize()]
                            at_least_one_ner_class = True
                            sequence.append((token.orth_, link, "B-" + ner_class))
                            sentence_str += token.orth_  +" X "+ "B-"+ner_class +" " +link+ " \n"

                        else:
                            ner_class = "O"
                            sequence.append((token.orth_, link, ner_class))
                            sentence_str += token.orth_ +" X " + ner_class +" " +link+ " \n"

                    except:
                        ner_class = "O"
                        sequence.append((token.orth_, None, ner_class))
                        sentence_str += token.orth_ + " X " + ner_class +" None \n"

            if at_least_one_ner_class:
                output.append(sequence)
                sequence_list_str += sentence_str +  " \n"
        self.parsed_text = output
        self.parsed_text_str = sequence_list_str





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