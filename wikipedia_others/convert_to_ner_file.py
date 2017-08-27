import argparse
import glob
import json
import os
import codecs

from tqdm import tqdm

def convert_to_ner_file(language, method, id, keep_table, subpart=None):
    all_sentences = []
    folder = "/dlabdata1/braemy/wikidataNER/"

    if subpart:
        input_file = os.path.join(folder, language, method,
                     "wikipedia_dataset_{0}{1}_{2}/part*".format("with_tables_" if keep_table else "", str(float(id)), subpart))

        output_file = os.path.join(folder, language, method,
                     "wikipedia_dataset_{0}{1}_{2}/combined_{3}_{1}_{2}.txt".format("with_tables_" if keep_table else "", str(float(id)), subpart, method))

    else:

        input_file = os.path.join(folder, language, method,
                                  "wikipedia_dataset_{0}{1}/part*".format("with_tables_" if keep_table else "",
                                                                              str(float(id))))

        output_file = os.path.join(folder, language, method,
                                   "wikipedia_dataset_{0}{1}/combined_{2}_{1}.txt".format(
                                       "with_tables_" if keep_table else "", str(float(id)), method))

    encoding= "utf-8"

    for file in tqdm(glob.glob(input_file)):
        with open(file, "r") as file:

            for line in file:
                line = json.loads(line)
                all_sentences.append(line['text'])
    with codecs.open(output_file, "w", encoding) as file:
        file.write("".join(all_sentences))
    print("File converted: ", output_file)

def predict_encoding(file_path, n_lines=20):
    '''Predict a file's encoding using chardet'''
    import chardet

    # Open the file as binary data
    with open(file_path, 'rb') as f:
        # Join binary lines for specified number of lines
        rawdata = b''.join([f.readline() for _ in range(n_lines)])

    return chardet.detect(rawdata)['encoding']


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=str, help='file id to process')
    parser.add_argument('--subpart', type=str, help='parse only subpart', default=None, required=False)
    parser.add_argument('--method', type=str, help='method to build dataset', choices=["wpb", "wp0", "wp2", "raw_page"])
    parser.add_argument('--language', type=str, help='method to build dataset', choices=["en", "de"])
    parser.add_argument('--file_type', type=str, required=False)

    args = parser.parse_args()
    convert_to_ner_file(args.language, args.method, args.id, args.subpart)