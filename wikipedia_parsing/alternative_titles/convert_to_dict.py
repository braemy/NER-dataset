import glob

import json
import sys

import pickle



input_file = "/dlabdata1/braemy/wikidataNER/alternative_titles/part*"
output_file = "/dlabdata1/braemy/wikidataNER/alternative_titles.json"
data = {}

with open("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p", "rb") as file:
    wp_to_ner_by_title =  pickle.load(file)
for i,file in enumerate(glob.glob(input_file)):
    if i %20 == 0:
        print(i)
    with open(file, "r") as file:

        for l in file:
            line = json.loads(l)
            title = line[0]
            alternative = line[1]
            #if title in wp_to_ner_by_title:
                #ner_class = wp_to_ner_by_title[title]
                #if ner_class == "PER":
                    #title_splited = title.split()
                    #if title_splited >=2:
                        #data[title_splited[0]] = title
                        #data[title_splited[-1]] = title
            data[title] = list(set(alternative))

with open(output_file, "w", encoding="utf-8") as file:
    json.dump(data, file)

print("File converted")
