import calendar
import datetime
import json
import os
import pickle
import time

import yaml
from tqdm import tqdm


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
        return pickle.load(file)
def pickle_data(data, file_name):
    with open(file_name, "wb") as file:
        pickle.dump(data,file,protocol=2)
def load_id_title(data_folder, id_):

    return load_pickle(os.path.join(data_folder, "id_title_dict_" + str(id_) + ".p"))
def load_title_id(data_folder, id_):
    return load_pickle(os.path.join(data_folder, "title_id_dict_"+ str(id_)+".p"))
def load_id_subclass(data_folder, id_):
    return load_pickle(os.path.join(data_folder, "id_subclass_of_dict_"+ str(id_)+".p"))
def load_id_instance(data_folder, id_):
    return load_pickle(os.path.join(data_folder, "id_instance_of_dict_"+ str(id_)+".p"))

def load_parameters(name=None):
    with open("../parameters.yml", 'r') as ymlfile:
        if name:
            return yaml.load(ymlfile)[name]
        else:
            return yaml.load(ymlfile)

def load_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

def convert_wp_to_(to_convert, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = load_json("/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json")

    # wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
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



def load_instance(folder):
    print("Loading id_to_instance")
    return load_pickle(os.path.join(folder, "id_instance_of_dict_0.p"))

def load_subclass(folder):
    print("Loading id_to_subclass")
    return load_pickle(os.path.join(folder, "id_subclass_of_dict_0.p"))

def load_id_to_title(folder):
    print("Loading id_to_title...")
    return load_pickle(os.path.join(folder, "id_title_dict_0.p"))


def load_title_to_id(folder):
    print("Loading title_to_id...")
    return load_pickle(os.path.join(folder, "title_to_dict_0.p"))

def is_date(token):
    return token.capitalize() in calendar.day_abbr or \
        token.capitalize() in calendar.day_name or \
        token.capitalize() in calendar.month_abbr or \
        token.capitalize() in calendar.month_name or \
        token in ["AD", "PM", "AM"]


def load_personal_titles():
    title = load_pickle("/dlabdata1/braemy/CoNLL/personal_titles.p")
    return title.union(constants.TITLE_ABBRS)

def load_sentence_starter():
    starter =  {s.title() for s in load_pickle("/dlabdata1/braemy/CoNLL/sentence_starter.p")}
    return starter.union(constants.SENTENCE_STARTER)
