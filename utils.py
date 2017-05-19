import os
import shutil
import pickle
import time

import datetime

import yaml


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
