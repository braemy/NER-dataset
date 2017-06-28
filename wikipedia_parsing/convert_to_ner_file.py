import argparse
import glob
import json


def main(args):
    all_sentences = ""

    if args.subpart:
        input_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/wikipedia_dataset_"+str(args.id)+args.subpart+"/part*"
        output_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/wikipedia_dataset_"+str(args.id)+args.subpart+"/combined.txt"
    else:
        input_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/wikipedia_dataset_"+str(args.id)+"/part*"
        output_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/wikipedia_dataset_"+str(args.id)+"/combined_"+args.method+"_"+str(args.id)+".txt"

    for file in glob.glob(input_file):

        with open(file, "r") as file:

            for line in file:
                line = json.loads(line)
                all_sentences += line['text']

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(all_sentences)
    print("File converted: ", output_file)

if __name__ == '__main__':
    if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument('--id', type=float, help='file id to process')
        parser.add_argument('--subpart', type=str, help='parse only subpart', default=None, required=False)
        parser.add_argument('--method', type=str, help='method to build dataset', choices=["wpb", "wp0", "wp2"])

        args = parser.parse_args()
        main(args)