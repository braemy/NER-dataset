# -*- coding: utf-8 -*-

import os
import re
import gzip
import codecs
from utils import *
from tqdm import tqdm




def map_wpID_to_wdId(parameters):
    #folder = "/dlabdata1/braemy/data"
    #file_name = "enwiki-20170301-wbc_entity_usage.sql"
    file_path = os.path.join(
        parameters["input_folder"],
        parameters["language"],
        "{0}wiki-{1}-page_props.sql.gz".format(
            parameters["language"],
            parameters["wiki_dump"],
        )
    )
    output_file_path = os.path.join(
        parameters["wikidataNER_folder"],
        parameters["language"],
        "wpId_to_wdId.p")

    with gzip.open(file_path, 'rt') as file:
        # (54732,'wikibase_item','Q69956',NULL)
        reg = r"\((\d+),'wikibase_item','(Q\d+)',.*?\)"
        pedia_to_data = dict()
        for line in tqdm(file):
            line = line.decode('iso-8859-1')
            results = re.finditer(reg, line)
            if not results:
                continue
            for result in results:
                # result = result.split(",")
                wikidata = result.group(2)
                wikipedia = int(result.group(1))
                pedia_to_data[wikipedia] = wikidata

        print("Number of mapping detected:", len(pedia_to_data))
        pickle_data(pedia_to_data, output_file_path)



def map_wpTitle_to_NER(parameters):
    print("Loading data...")
    wpId_to_wdId = load_pickle("{0}/{1}/wpId_to_wdId.p".format(parameters["wikidataNER_folder"], parameters["language"]))
    wpTitle_to_wpId = load_wpTitle_to_wpId(parameters)

    output_path = "{0}/{1}/wp_to_ner_by_title.p".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    )
    output_path_titles = "{0}/{1}/personal_titles.p".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    )
    output_path_nationalities = "{0}/{1}/nationalities.p".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    )
    output_path_calendar = "{0}/{1}/calendar.p".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    )
    mapping_wd_to_NER = load_pickle(parameters['wd_to_NER'])
    personal_titles_wdID = set(load_pickle(os.path.join(parameters['wikidataNER_folder'], 'personal_titles_wdID.p')))
    nationalities_wdID = set(load_pickle(os.path.join(parameters['wikidataNER_folder'], 'nationalities_wdID.p')))
    calendar_wdID = set(load_pickle(os.path.join(parameters['wikidataNER_folder'], 'calendar_wdID.p')))

    wp_to_ner_by_title = dict()
    personal_titles = set()
    nationalities = set()
    calendar = set()

    for k_title, infos in tqdm(wpTitle_to_wpId.items()):
        if any(l in k_title for l in parameters["list_of"]):
            continue
        try:
            wdID = wpId_to_wdId[infos['id']]
            if wdID in nationalities_wdID:
                nationalities.add(k_title)
            if wdID in calendar_wdID:
                calendar.add(k_title)
            if wdID in personal_titles_wdID:
                if len(k_title.split()) == 1:
                    personal_titles.add(k_title)
            wp_to_ner_by_title[k_title] = {'ner':mapping_wd_to_NER[wdID], 'lc': infos['lc']}
        except KeyError:
            continue
    print("Number of entities:", len(wp_to_ner_by_title))
    pickle_data(wp_to_ner_by_title, output_path)
    pickle_data(personal_titles, output_path_titles)
    pickle_data(nationalities, output_path_nationalities)
    pickle_data(calendar, output_path_calendar)

