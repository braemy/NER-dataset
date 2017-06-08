import argparse
import glob
import json


def main(id, subpart=None):
    all_sentences = ""

    if subpart:
        input_file = "/dlabdata1/braemy/wikidataNER/wikipedia_dataset_"+str(id)+subpart+"/part*"
        output_file = "/dlabdata1/braemy/wikidataNER/wikipedia_dataset_"+str(id)+subpart+"/combined.txt"
    else:
        input_file = "/dlabdata1/braemy/wikidataNER/wikipedia_dataset_"+str(id)+"/part*"
        output_file = "/dlabdata1/braemy/wikidataNER/wikipedia_dataset_"+str(id)+"/combined.txt"

    for file in glob.glob(input_file):

        with open(file, "r") as file:

            for line in file:
                line = json.loads(line)
                all_sentences += line['text']

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(all_sentences)
    print("File converted")

if __name__ == '__main__':
    if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument('--id', type=int, help='file id to process')
        parser.add_argument('--subpart', type=str, help='parse only subpart', default=None, required=False)

        args = parser.parse_args()
        main(args.id, args.subpart)