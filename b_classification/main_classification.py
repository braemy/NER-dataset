import  sys
sys.path.append("../")
from utils import *

from Wikidata_classification import Wikidata_classification
import os
if __name__ == '__main__':
    print("Wikidata b_classification")
    #wd_classification = Wikidata_classification()
    # wd_classification.build_mapping_to_NER_class()
    #wd_classification.classify_article()

    convert_wp_to_("/dlabdata1/braemy/wikidata-b_classification/mapping_wikidata_to_NER.p", "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")