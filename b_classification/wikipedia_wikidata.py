# -*- coding: utf-8 -*-

import os
import re
from utils import *
from tqdm import tqdm



def map_wpID_to_wdId(parameters):
    #folder = "/dlabdata1/braemy/data"
    #file_name = "enwiki-20170301-wbc_entity_usage.sql"
    file_path = os.path.join(
        parameters["input_folder"],
        parameters["language"],
        "{0}wiki-{1}-wbc_entity_usage.sql".format(
            parameters["language"],
            parameters["wiki_dump"],
        )
    )
    output_file_path = os.path.join(
        parameters["wikidataNER_folder"],
        parameters["language"],
        "wpId_to_wdId.p")

    with open(file_path, "r") as file:
        data = file.read()

        # (301,'Q277941','X',1190,'\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0')
        reg = r"\d+,'Q\d+','\w',\d+"
        result = re.findall(reg, data)
        len(result)

        pedia_to_data = dict()
        for line in tqdm(result):
            line = line.split(",")
            wikidata = line[1].strip("'")
            wikipedia = int(line[3])
            pedia_to_data[wikipedia] = wikidata
        pickle_data(pedia_to_data, output_file_path)


def map_wpTitle_to_NER(parameters):
    print("Loading data...")
    wpId_to_wdId = load_pickle("{0}/{1}/wpId_to_wdId.p".format(parameters["wikidataNER_folder"], parameters["language"]))
    wpTitle_to_wpId = load_json("{0}/{1}/wpTitle_to_wpId.json".format(parameters["wikidataNER_folder"], parameters["language"]))
    output_path = "{0}/{1}/wp_to_ner_by_title.p".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    )
    mapping_wd_to_NER = load_pickle(parameters['wd_to_NER'])
    # wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
    wp_to_ner_by_title = dict()

    #for title, ner_class in map_wpTitle_to_NER.items():
    #    if any(l in k_title for l in parameters["list_of"]):
    #        continue
    #    try:



    for k_title, infos in tqdm(wpTitle_to_wpId.items()):
        if any(l in k_title for l in parameters["list_of"]):
            continue
        try:
            wp_to_ner_by_title[k_title] = {'ner':mapping_wd_to_NER[wpId_to_wdId[infos['id']]], 'lc': infos['lc']}
        except KeyError:
            continue



    print("Number of entities:", len(wp_to_ner_by_title))
    # pickle_data(wp_to_ner_by_title, "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
    pickle_data(wp_to_ner_by_title, output_path)

