# -*- coding: utf-8 -*-
"""
Created by e-bug on 07/03/17.
"""
import sys

from pages_manager import Pages_manager
from utils import *
from wikipedia_iterator import Wikipedia_iterator

wiki_iterator = Wikipedia_iterator()
pages_manager = Pages_manager()
page = True
#wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
#wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
#wikiTitle_to_id = load_pickle("/dlabdata1/braemy/wikipedia_classification/title_to_id_170.p")

wp_to_ner_by_title = load_pickle("/dlabdata1/braemy/wikidataNER/en/wp_to_ner_by_title.p")
#alternative_titles = load_json("/dlabdata1/braemy/wikidataNER/alternative_titles.json")
#personal_titles =load_personal_titles()
#sentence_starters =load_sentence_starter()
while page is not None:
    try:
        page =wiki_iterator.next_page()
        if page.is_redirect() or page.title== "NO-TITLE":
            continue
        if page.id is not None and int(page.id) %10000 == 0:
            print(page.id)
        print(page.title)
        #if page.title != "Academy Award for Best Production Design":
        #   continue
        #if "{{Color box" in page.get_text() and page.title not in ["Age of consent", "Crandall University"]:
        #print("Title", page.title)
        #print(page.get_text())
        #else:
        #    continue
        #line = {"title": page.title, "text":page.get_text()}
        #collect_title(line, wp_to_ner_by_title)
        cleaned_text, title_to_alternatives = page.clean(wp_to_ner_by_title)
        #print(cleaned_text)
        #parsed_text = page.parse(wp_to_ner_by_title=wp_to_ner_by_title, alternative_titles=alternative_titles, personal_titles=personal_titles, sentence_starter=sentence_starters)
        #print(parsed_text)

        #print(text)
        #pages_manager.add_page(page)
    except StopIteration:
        pages_manager.save_all()

pages_manager.save_all()