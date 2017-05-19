import glob

import json

all_sentences = ""

for file in glob.glob("/dlabdata1/braemy/wikidataNER/wikipedia_dataset/part*"):
    with open(file, "r", encoding="iso-8859-1") as file:
        for line in file:
            line = json.loads(line)
            all_sentences += line['text']

with open("/dlabdata1/braemy/wikidataNER/wikipedia_dataset/combined.txt", "w", encoding="utf-8") as file:
    file.write(all_sentences)