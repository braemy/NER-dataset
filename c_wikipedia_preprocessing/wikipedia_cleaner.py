import sys
sys.path.append("../")
#from wikipedia_others.c_wikipedia_preprocessing.helper_remove import *
#from wikipedia_others.c_wikipedia_preprocessing.helper_replace import *
#from wikipedia_others.c_wikipedia_preprocessing.wiki_infos import *
#from wikipedia_others.c_wikipedia_preprocessing.alternative_titles import *

from helper_remove import *
from helper_replace import *
from wiki_infos import *
from alternative_titles import *

import nltk
nltk.data.path.append('/home/braemy/nltk_data/')

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *



class Wikipedia_cleaner(object):
    def __init__(self, row):
        self.text = row['text']
        self.title = row['title']
        self.id = row['id']

    def is_redirect_(self):
        return "#REDIRECT" in self.text

    def clean_localy(self, wp_to_ner_by_title):
        disamb = is_disambiguation(self.text)
        if disamb:
            title_to_alternative_title = collect_title_disambiguation(self.title,self.text, wp_to_ner_by_title)
            return title_to_alternative_title, None
        else:
            out_going_link = collect_all_outgoing_link(self.text, wp_to_ner_by_title)
            title_redirect = collect_title_redirect(self.title, self.text, wp_to_ner_by_title)
            parsed = self.apply_regex(self.text)
            title_bold = collect_title_first_paragraph(self.title, parsed, wp_to_ner_by_title)
            return {self.title:list(title_redirect.union(title_bold))}, out_going_link
        #return text, out_going_link

    def collect_outgoing_link(self, wp_to_ner_by_title):
        text = replace_html_tags(self.text)
        disamb = is_disambiguation(text)
        if disamb:
            title_to_alternative_title = collect_title_disambiguation(self.title, text, wp_to_ner_by_title)
            out_going_link = dict()
        else:
            out_going_link = collect_all_outgoing_link(text, wp_to_ner_by_title)
            title_to_alternative_title = collect_title_redirect(self.title, text, wp_to_ner_by_title)
            title_to_alternative_title = {self.title: list(title_to_alternative_title)}
        return Row(title=self.title, disamb=int(disamb), alt=title_to_alternative_title,
                   links=out_going_link)

    def clean_wikiExtractor(self):
        #TODO copy command line wikiextractor without printing
        return -1



    def clean_spark(self, wp_to_ner_by_title):
        text = replace_html_tags(self.text)
        disamb = is_disambiguation(text)
        if disamb:
            title_to_alternative_title = collect_title_disambiguation(self.title, text, wp_to_ner_by_title)
            cleaned_text = self.apply_regex(self.text)
            out_going_link = dict()
        else:
            out_going_link = collect_all_outgoing_link(text, wp_to_ner_by_title)
            title_redirect = collect_title_redirect(self.title, text, wp_to_ner_by_title)
            cleaned_text = self.apply_regex(self.text)
            title_bold = collect_title_first_paragraph(self.title, cleaned_text, wp_to_ner_by_title)
            title_to_alternative_title =  {self.title: list(title_redirect.union(title_bold))}

        return Row(title=self.title, text = cleaned_text, disamb=int(disamb), alt=title_to_alternative_title, links=out_going_link)
        #except RuntimeError:
        #    return Row(title=self.title, text=self.text, error=1)




    def apply_regex(self, text):
        # Replace stuff
        text = replace_wikt(text)
        text = replace_lang(text)
        text = replace_code(text)
        text = replace_tt(text)
        text = replace_s(text)
        text = replace_list_bullets(text)
        text = replace_div(text)
        text = clean_table(text)

        # Remove stuff
        text = remove_block(text, r"\{\{", r"\}\}")
        #text = remove_block(text, r"\{\|", r"\|\}")
        text = remove_block(text, r"<ref[^/]*?>", r"<\/ref>")
        # Remove stuff with specific balise first surch as {{algo-begin}}... {algo-end}} and then remove braces
        text = remove_see_also(text)
        text = remove_algorithm(text)
        text = remove_comment(text)
        text = remove_html_balise(text)

        text = remove_nowiki(text)
        text = remove_center(text)
        text = remove_math(text)
        text = remove_sub(text)
        text = remove_image(text)
        text = remove_blockquote(text)
        text = remove_sup(text)
        text = remove_small(text)
        text = remove_big(text)
        text = remove_gallery(text)
        text = remove_italic(text)
        text = remove_source(text)

        text = remove_section(text)
        text = remove_categories(text)
        text = remove_empty_parenthesis(text)
        text = remove_ref(text)

        text = remove_br(text)
        text = remove_list(text)
        text = remove_quote(text)
        text = remove_others(text)
        text = remove_parenthesis_first_sentence(text)
        text = remove_table(text)

        text = text.replace("\n\n\n", "")
        text = text.replace("\n\n", ". ")
        return text














