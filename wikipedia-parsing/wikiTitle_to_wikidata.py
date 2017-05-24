from tqdm import tqdm
import json
from utils import *


def convert_wp_to_(destination_path, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = load_pickle("/dlabdata1/braemy/wikipedia_classification/title_to_id_170.p")

    #wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
    wikidata_to_ner = load_pickle(destination_path)
    wp_to_ner_by_title = dict()
    for k_title,v_id in tqdm(wikiTitle_to_id.items()):
        try:
            wp_to_ner_by_title[k_title] = wikidata_to_ner[wikipedia_to_wikidata[v_id]]
        except:
            continue

    print(len(wp_to_ner_by_title))
    #pickle_data(wp_to_ner_by_title, "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
    pickle_data(wp_to_ner_by_title, output_path)
