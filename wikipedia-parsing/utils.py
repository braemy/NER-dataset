import pickle

import time

import datetime

import sparkpickle

def pickle_data(data, file_name):
    with open(file_name, "wb") as file:
        pickle.dump(data, file, protocol=2)

def load_pickle(file_name):
    with open(file_name, "rb") as file:
        return pickle.load(file)

def load_pickle_spark(file_name):
    with open(file_name, "rb") as file:
        return sparkpickle.load(file)

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
