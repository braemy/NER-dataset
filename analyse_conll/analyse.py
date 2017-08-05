import sys
from pprint import pprint

sys.path.append("..")
import string
import utils
import os
import locale
import constants as constants
from collections import Counter

def personal_titles(parameters):
    language = parameters['language']
    threshold = parameters['personal_title_threshold']
    print("Personale titles:", language)
    input_folder = "/dlabdata1/braemy/nerc-conll2003"
    encoding = "utf-8"
    if language == "en":
        file_name = os.path.join(input_folder, "eng.train")
    elif language == "de":
        file_name = os.path.join(input_folder, "deu.train")
    utils.create_directory(os.path.join(input_folder, language))
    output_file = os.path.join(parameters["wikidataNER_folder"], language, "personal_titles.p")

    with open(file_name, "r") as file:
        before_per = []
        previous_token = ""
        previous_label = ""
        position = 0
        for line in file:
            line_list = line.strip().split()
            if line_list:
                token = line_list[0]
                label = line_list[-1]
                if position!=1 and label == "B-PER":
                    if previous_token != "" and previous_token not in string.punctuation and previous_token[0].isupper():
                        if previous_token in constants.NOT_TITLE_EN or previous_label == "B-MISC" or utils.is_date(previous_token, language): # => adjectif like American
                            continue
                        else:
                            before_per.append(previous_token)
                else:
                    position += 1
                previous_token = token
                previous_label = label
            else:
                position = 0

        #keep only token that appears at least k times
        set_before_per = {k for k,v in Counter(before_per).items() if v > threshold}
        print(set_before_per)
        utils.pickle_data(set_before_per, output_file)

def sentence_starter(parameters):
    language = parameters['language']
    print("Sentence starter for:", language)
    input_folder = "/dlabdata1/braemy/nerc-conll2003"
    encoding = "utf-8"
    if language == "en":
        file_name = os.path.join(input_folder, "eng.train")
    elif language in ["de","als"]:
        file_name = os.path.join(input_folder, "deu.train")
    utils.create_directory(os.path.join(input_folder, language))
    output_file = os.path.join(parameters['wikidataNER_folder'], language, "sentence_starter.p")

    with open(file_name) as file:
        starter = dict()
        start = True
        for line in file:
            tokens = line.split()
            if tokens:
                if start:
                    word = tokens[0]
                    label = tokens[-1]
                    if word.isdigit() or not word.istitle() or word.islower() or label != "O":
                        start = False
                        continue
                    if word in string.punctuation:
                        continue
                    start = False
                    if word not in starter:
                        starter[word] = {'start':1, 'inside':0}
                    else:
                        starter[word]['start'] += 1
            else:
                start = True
    with open(file_name) as file:
        start = True
        for line in file:
            tokens = line.split()
            if tokens:
                if start:
                    start = False
                    continue
                word = tokens[0]
                if word in starter:
                    starter[word]['inside'] +=1
            else:
                start = True
        starter = {k.title() for k,v in starter.items() if v['inside']< 10 and v['start'] > 10}
        utils.pickle_data(starter, output_file)
        print(starter)

if __name__ == '__main__':
    languages = [("en", 1), ("de",3)]
    parameters = utils.load_parameters()
    for language, threshold in languages:
        sentence_starter(parameters, language)
        personal_titles(parameters, language, threshold)