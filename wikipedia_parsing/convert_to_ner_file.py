import argparse
import glob
import json


def main(args):
    all_sentences = ""
    all_titles = ""

    if args.file_type is not None:
        file_name = args.file_type
    else:
        file_name = "wikipedia_dataset"
    if args.subpart:
        input_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/"+file_name+"_"+str(args.id)+"_"+args.subpart+"/part*"
        output_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/"+file_name+"_"+str(args.id)+"_"+args.subpart+"/combined_"+args.method+"_"+str(args.id)+"_"+args.subpart+".txt"
    else:
        input_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/"+file_name+"_"+str(args.id)+"/part*"
        output_file = "/dlabdata1/braemy/wikidataNER/"+args.method+"/"+file_name+"_"+str(args.id)+"/combined_"+args.method+"_"+str(args.id)+".txt"

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
        parser.add_argument('--id', type=str, help='file id to process')
        parser.add_argument('--subpart', type=str, help='parse only subpart', default=None, required=False)
        parser.add_argument('--method', type=str, help='method to build dataset', choices=["wpb", "wp0", "wp2", "raw_page"])
        parser.add_argument('--file_type', type=str, required=False)

        args = parser.parse_args()
        main(args)