import glob

import json

input_file = "/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId/part*"
output_file = "/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json"
data = {}
for file in glob.glob(input_file):

    with open(file, "r") as file:

        for line in file:
            line = json.loads(line)
            #data[line['title']] = {'id': line['id'], 'lc':line['lc'], 'wd': line['wd']}
            data[line['title']] = {'id': line['id'], 'lc':line['lc']}

with open(output_file, "w", encoding="utf-8") as file:
    json.dump(data, file)

print("File converted")
