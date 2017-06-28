import os
import re
from utils import *
from tqdm import tqdm

folder = "/dlabdata1/braemy/data"
file_name = "enwiki-20170301-wbc_entity_usage.sql"

with open(os.path.join(folder, file_name), "r") as file:
    data = file.read()

    # (301,'Q277941','X',1190,'\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0')
    reg = r"\d+,'Q\d+','\w',\d+"
    result = re.findall(reg, data)
    len(result)

    pedia_to_data = dict()
    data_to_pedia = dict()
    for line in tqdm(result):
        line = line.split(",")
        wikidata = line[1].strip("'")
        wikipedia = int(line[3])
        pedia_to_data[wikipedia] = wikidata
    pickle_data(pedia_to_data, "/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")