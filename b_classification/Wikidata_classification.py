# -*- coding: utf-8 -*-

from b_classification.NER_mapping import NER_mapping
from utils import *


class Wikidata_classification(object):
    def  __init__(self, parameters):
        self.parameter_wd = load_parameters('wikidata')
        self.parameter_gr = load_parameters()
        self.folder = self.parameter_wd['output_dir']
        self.id_to_instance = dict()

        self.parameters = parameters

    def load_instance(self):
        self.id_to_instance = load_instance(self.parameters)

    def load_subclass(self):
        self.id_to_instance = load_subclass(self.parameters)

    def load_id_to_title(self):
        self.id_to_title = load_id_to_title(self.folder)

    def load_title_to_id(self):
        self.title_to_id = load_title_to_id(self.folder)

    def classify_article(self):
        self.load_instance()
        ner_mapping = NER_mapping(self.parameters)
        id_to_nerClass = dict()
        for id_, instances in tqdm(self.id_to_instance.items()):
            ner_class, instances = ner_mapping.classify_entity_by_instances(instances)
            if ner_class:
                id_to_nerClass[id_] = ner_class
            elif ner_mapping.is_loc(id_):
                id_to_nerClass[id_] = "LOC"
            elif ner_mapping.is_org(id_):
                id_to_nerClass[id_] = "ORG"

        pickle_data(id_to_nerClass, self.parameters['wd_to_NER'])

        #convert_wp_to_(self.parameter_wd['wd_to_NER'], "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
        #convert_wp_to_(self.parameter_wd['wd_to_NER'], os.path.join(self.parameters["wikidataNER_folder"], self.parameters["language"], "wp_to_ner_by_title.p"))

    def get_subset_of_wikidata(self, list_of_id, name):
        file_path = os.path.join("/dlabdata1/braemy/wikidata-b_classification/", name + ".p")
        # self.build_mapping(list_of_id, file_path)

        self.load_instance()
        wd_id_of_subclasses = load_pickle(file_path)
        wd_set_output = set()
        for id_, instances in tqdm(self.id_to_instance.items()):
            for instance in instances:
                if instance in wd_id_of_subclasses:
                    wd_set_output.add(id_)
        convert_wd_id_to_wp_title(wd_set_output,
                                  "/dlabdata1/braemy/wikipedia_classification/wp_by_title_" + name + ".p")

