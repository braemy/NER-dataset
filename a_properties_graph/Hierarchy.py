import os
import pickle
import shutil
from utils import load_parameters


class Hierarchy(object):
    def __init__(self, output_dir):
        self.id_subclass_of_dict = dict()
        self.id_instance_of_dict = dict()
        self.id_to_title = dict()
        self.title_to_id = dict()
        self.dict_id = 0
        self.output_dir = output_dir
        self.parameters = load_parameters('wikidata')



    def pickle_and_reset(self, dict_name, name):
        with open(os.path.join(self.output_dir, name + '_' + str(self.dict_id) + '.p'), 'wb') as fp:
            pickle.dump(getattr(self, dict_name), fp)
            getattr(self, dict_name).clear()

    def save_dicts(self):
        self.pickle_and_reset('id_subclass_of_dict', self.parameters['id_to_subclass'])
        self.pickle_and_reset('id_instance_of_dict', self.parameters['id_to_instance'])
        self.pickle_and_reset('id_to_title', self.parameters['id_to_title'])
        self.pickle_and_reset('title_to_id', self.parameters['title_to_id'])
        print("Dicts number:", self.dict_id, "saved")
        self.dict_id += 1

    def add_id_title(self,id_, title ):
        self.id_to_title[id_] = title
        self.title_to_id[title] = id_

    def add_instance_subclass(self, id_, title, instance_of_list, subclass_of_list):
        if subclass_of_list:
            self.id_subclass_of_dict[id_] = subclass_of_list
        if instance_of_list:
            self.id_instance_of_dict[id_] = instance_of_list

