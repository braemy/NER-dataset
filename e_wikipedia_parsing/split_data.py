import codecs
import glob
import json
import random
import os
import constants

import sys
from tqdm import tqdm

def split_train_valid_test(parameters):
    all_sentences = []
    folder = parameters['wikidataNER_folder']
    subpart = parameters['subpart'] if 'subpart' in parameters else None
    language = parameters['language']
    method = parameters['method']
    keep_table = parameters['keep_table']
    filter = parameters['filter']

    if subpart:
        input_file = os.path.join(folder, language, method,
                     "wikipedia_dataset_{0}{1}_{2}/part*".format("with_tables_" if keep_table else "", str(float(filter)), subpart))

        output_file = os.path.join(folder, language, method,
                     "wikipedia_dataset_{0}{1}_{2}/combined_{3}_{1}_{2}.txt".format("with_tables_" if keep_table else "", str(float(filter)), subpart, method))

    else:

        input_file = os.path.join(folder, language, method,
                                  "wikipedia_dataset_{0}{1}/part*".format("with_tables_" if keep_table else "",
                                                                              str(float(filter))))

        output_file = os.path.join(folder, language, method,
                                   "wikipedia_dataset_{0}{1}/combined_{2}_{1}.".format(
                                       "with_tables_" if keep_table else "", str(float(filter)), method))


    encoding= "utf-8"
    data = dict()
    for file in tqdm(glob.glob(input_file)):
        with open(file, "r") as file:
            for line in file:
                line = json.loads(line)
                data[line['title']] = line['text']

    keys = list(data.keys())
    random.shuffle(keys)
    data = {k: data[k] for k in keys}

    if language in constants.LANGUAGE_FULL_NAME:
        split_big_file(data, output_file)
    else:
        split_small_file(data, output_file)

def split_big_file(data):
    dataset_types = ['train', 'valid', 'test']
    output_data = []
    token_counter = 0
    titles = []
    dataset_type = dataset_types.pop()
    for title, sentences in data.items():
        if sentences.strip():
            if token_counter >= parameters["token_" + dataset_type]:
                tmp_file = output_file + dataset_type
                with codecs.open(tmp_file, "w", "utf-8") as file:
                    for line in output_data:
                        file.write(line)
                        file.write("\n")
                tmp_file = tmp_file + "_"+ "titles"
                with codecs.open(tmp_file, "w", "utf-8") as file:
                    for t in titles:
                        file.write(t +"\n")
                output_data = []
                titles = []

                token_counter = 0
                if dataset_types:
                    dataset_type = dataset_types.pop()
                    print(dataset_type)
                else:
                    break
            titles.append(title)
            token_counter += len(sentences.strip().split("\n"))
            output_data.append(sentences)

    if output_data:
        tmp_file = output_file + dataset_type
        with codecs.open(tmp_file, "w", "utf-8") as file:
            for line in output_data:
                file.write(line)
                file.write("\n")
        tmp_file = tmp_file + "_" + "titles"
        with codecs.open(tmp_file, "w", "utf-8") as file:
            for t in titles:
                file.write(t + "\n")
        print("Number of token not achieved:", token_counter)

    print(output_file, "created")

def split_small_file(data, output_file):
    keys = data.keys()
    train = {k: data[k] for k in keys[0:int(0.8*len(keys))]}
    valid = {k: data[k] for k in keys[int(0.8*len(keys)):int(0.9*len(keys))]}
    test = {k: data[k] for k in keys[int(0.9*len(keys)):]}

    for data, dataset_type in [(train,'train'), (valid, 'valid'), (test, 'test')]:
        output_data = []
        titles = []
        token_counter = 0
        for title, sentences in data.items():
            if sentences.strip():
                titles.append(title)
                token_counter += len(sentences.strip().split("\n"))
                output_data.append(sentences)

        tmp_file = output_file + dataset_type
        with codecs.open(tmp_file, "w", "utf-8") as file:
            for line in output_data:
                file.write(line)
                file.write("\n")
        tmp_file = tmp_file + "_" + "titles"
        with codecs.open(tmp_file, "w", "utf-8") as file:
            for t in titles:
                file.write(t + "\n")

        print(output_file, "created", token_counter, "tokens")