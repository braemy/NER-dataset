import calendar
import codecs
import datetime
import json
import os
import pickle
import time
import locale
import subprocess
import shutil

import yaml
from tqdm import tqdm
from glob import glob

import sys
sys.path.append(".")
import constants


def get_current_milliseconds():
    '''
    http://stackoverflow.com/questions/5998245/get-current-time-in-milliseconds-in-python
    '''
    return(int(round(time.time() * 1000)))


def get_current_time_in_seconds():
    '''
    http://stackoverflow.com/questions/415511/how-to-get-current-time-in-python
    '''
    return(time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()))


def get_current_time_in_miliseconds():
    '''
    http://stackoverflow.com/questions/5998245/get-current-time-in-milliseconds-in-python
    '''
    return(get_current_time_in_seconds() + '-' + str(datetime.datetime.now().microsecond))

def create_folder(output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

def load_pickle(name):
    with open(name, "rb") as file:
        file = pickle.load(file)
        print("{} loaded".format(name))
        return file
def pickle_data(data, file_name):
    with open(file_name, "wb") as file:
        pickle.dump(data,file,protocol=2)
    print("{} saved!".format(file_name))

def load_parameters(name=None):
    with open("parameters.yml", 'r') as ymlfile:
        if name:
            return yaml.load(ymlfile)[name]
        else:
            return yaml.load(ymlfile)

def load_parameters_dataset_builder(language, parameters_file_path):
    with codecs.open(parameters_file_path, 'r', encoding="utf-8") as ymlfile:
        para = yaml.load(ymlfile)['dataset_builder']
        para_language = para[language]
        para_language["wikidataNER_folder"] = para['wikidataNER_folder']
        para_language["input_folder"] = para['input_folder']
        para_language["wd_to_NER"] = para['wd_to_NER']
        para_language["hadoop_folder"] = para['hadoop_folder']
        para_language["wikidata_dump"] = para['wikidata_dump']
        para_language["conll_folder"] = para['conll_folder']
        if "filter" in para_language:
            para_language["filter"] = float(para_language["filter"])
        return para_language

def load_json(file_path):
    with open(file_path, "r") as file:
        file =  json.load(file)
        print("{} loaded".format(file_path))
        return file
def dump_json(data, file_path):
    with open(file_path, "w") as file:
        json.dump(data, file)
    print("{} saved".format(file_path))

def convert_wp_to_(to_convert, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = load_json("/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json")

    # wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-b_classification/mapping_wikidata_to_NER.p")
    wikidata_to_ner = load_pickle(to_convert)
    wp_to_ner_by_title = dict()
    for k_title, infos in tqdm(wikiTitle_to_id.items()):
        try:
            wp_to_ner_by_title[k_title] = {'ner':wikidata_to_ner[wikipedia_to_wikidata[infos['id']]], 'lc': infos['lc']}
        except KeyError:
            continue

    print("Number of entities:", len(wp_to_ner_by_title))
    # pickle_data(wp_to_ner_by_title, "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
    pickle_data(wp_to_ner_by_title, output_path)

def convert_wd_id_to_wp_title(wikidata_set, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = load_json("/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json")

    wikidata_set_title = set()
    for k_title, infos in tqdm(wikiTitle_to_id.items()):
        try:
            if wikipedia_to_wikidata[str(infos['id'])] in wikidata_set:
                wikidata_set_title.add(k_title)
        except:
            continue
    print("Number of entities:",len(wikidata_set_title))
    pickle_data(wikidata_set_title, output_path)



def load_instance(parameters):
    return load_json_by_line(parameters, "id_to_instanceOf", "id", "instanceOf")

def load_subclass(parameters):
    return load_json_by_line(parameters, "id_to_subclassOf", "id", "subclassOf")

def load_id_to_loc(parameters):
    return load_json_by_line(parameters, "id_to_loc", "id", "loc")

def load_id_to_org(parameters):
    return load_json_by_line(parameters, "id_to_org", "id", "org")

def load_json_by_line(parameters, file_name, key, value):
    print("Loading", file_name)
    file_paths = os.path.join(parameters["wikidataNER_folder"],
                              file_name, "part*")
    id_to_subclass = dict()
    for file_path in glob(file_paths):
        print(file_path)
        with open(file_path, "r") as file:
            for line in tqdm(file):
                line = json.loads(line)
                id_to_subclass[line[key]] = line[value]
    return id_to_subclass

    #return load_pickle(os.path.join(parameters["wikidataNER_folder"],
    #                                "id_to_subclassOf.p"))




def load_id_to_title(folder):
    print("Loading id_to_title...")
    return load_pickle(os.path.join(folder, "id_title_dict_0.p"))


def load_title_to_id(folder):
    print("Loading title_to_id...")
    return load_pickle(os.path.join(folder, "title_to_dict_0.p"))

def is_date(token, language):
    if language == "en":
        locale.setlocale(locale.LC_ALL, "en_US")
    elif language in ["de", "als"]:
        locale.setlocale(locale.LC_ALL, "de_DE")
    else:
        raise("Unrecognized language")

    return token.capitalize() in calendar.day_abbr or \
        token.capitalize() in calendar.day_name or \
        token.capitalize() in calendar.month_abbr or \
        token.capitalize() in calendar.month_name or \
        token in ["AD", "PM", "AM", "Century", "Millenium"]


def load_personal_titles(language):
    try:
        title = load_pickle(os.path.join("/dlabdata1/braemy/wikidataNER", language, "personal_titles.p"))
        if language == "en":
            return title.union(constants.TITLE_ABBRS_EN)
        return title
    except:
        return set()

def load_sentence_starter(language):
    try:
        starter =  {s.title() for s in load_pickle(os.path.join("/dlabdata1/braemy/wikidataNER", language, "sentence_starter.p"))}
        if language == "en":
            return starter.union(constants.SENTENCE_STARTER_EN)
        return starter
    except:
        return set()

def split_tag(ner_class):
    if ner_class == "O":
        return None, "O"
    return ner_class.split("-")


def is_capitalized(token):
    return token[0].isupper()

def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_mapping_DE_ALS():
    ger = "script_swiss_german/word-de.train.de.punc.txt"
    als = "script_swiss_german/word-de.train.gsw.punc.txt"

    with codecs.open(ger, "r", encoding="utf-8") as file:
        ger_data = [g.lstrip(":") for g in file]
    with codecs.open(als, "r", encoding="utf-8") as file:
        als_data = [g.lstrip(":") for g in file]

    mapping = dict()
    for g, a in zip(ger_data, als_data):
        mapping[a] = g

    return mapping

def is_disambiguation(title):
    return "(disambiguation)" in title

def is_list_of(title, parameters):
    for list_of in parameters["list_of"]:
        if list_of.lower().split() == title.lower().split()[:len(list_of.split())]:
            return True
    return False

def hadoop_rm(parameters, file_name):
    command = "hadoop fs -rm -r {0}/{1}".format(
        parameters["hadoop_folder"],
        file_name)
    subprocess.call(command, shell=True)

def hadoop_get(parameters,file_name):
    dir_path = "{0}/{1}".format(
        parameters['wikidataNER_folder'],
        file_name
    )
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)

    command = "hadoop fs -get {0}/{2}.json {1}/{2}".format(
        parameters["hadoop_folder"],
        parameters["wikidataNER_folder"],
        file_name
    )
    subprocess.call(command, shell=True)

