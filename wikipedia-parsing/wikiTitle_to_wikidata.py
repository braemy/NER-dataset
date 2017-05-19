from tqdm import tqdm
import json
from utils import *

wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
wikiTitle_to_id = load_pickle("/dlabdata1/braemy/wikipedia_classification/title_to_id_170.p")

#wp_to_wd_by_title = dict()

#for k,v in tqdm(wikiTitle_to_id.items()):
#    try:
#        wp_to_wd_by_title[k] = wikipedia_to_wikidata[v]
#    except:
#        continue

#print(len(wp_to_wd_by_title))
#pickle_data(wp_to_wd_by_title, "/dlabdata1/braemy/wikipedia_classification/wp_to_wd_by_title")



wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
wp_to_ner_by_title = dict()
for k_title,v_id in tqdm(wikiTitle_to_id.items()):
    try:
        wp_to_ner_by_title[k_title] = wikidata_to_ner[wikipedia_to_wikidata[v_id]]
    except:
        continue

print(len(wp_to_ner_by_title))
#pickle_data(wp_to_ner_by_title, "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")

with open("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.json", 'w') as fp:
    json.dump(wp_to_ner_by_title, fp)
