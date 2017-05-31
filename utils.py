import os
import shutil
import pickle
import time

import datetime

import json
import yaml
from tqdm import tqdm


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
def pickle_file(file_name, data):
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
    with open("parameters.yml", 'r') as ymlfile:
        if name:
            return yaml.load(ymlfile)[name]
        else:
            return yaml.load(ymlfile)

def json_load(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

def convert_wp_to_(destination_path, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = json_load("/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json")

    # wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
    wikidata_to_ner = load_pickle(destination_path)
    wp_to_ner_by_title = dict()
    for k_title, infos in tqdm(wikiTitle_to_id.items()):
        print(k_title, infos)
        try:
            wp_to_ner_by_title[k_title] = {'ner':wikidata_to_ner[wikipedia_to_wikidata[infos]], 'lc': infos['lc']}
        except:
            continue

    print("Number of entities:", len(wp_to_ner_by_title))
    # pickle_data(wp_to_ner_by_title, "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
    pickle_file(output_path,wp_to_ner_by_title )

def convert_wd_id_to_wp_title(wikidata_set, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = json_load("/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json")

    wikidata_set_title = set()
    for k_title, infos in tqdm(wikiTitle_to_id.items()):
        try:
            if wikipedia_to_wikidata[infos['id']] in wikidata_set:
                wikidata_set_title.add(k_title)
        except:
            continue
    print("Number of entities:",len(wikidata_set_title))
    pickle_file(output_path, wikidata_set_title)