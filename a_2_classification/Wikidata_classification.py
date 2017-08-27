# -*- coding: utf-8 -*-

from a_2_classification.NER_mapping import NER_mapping
from utils import *


class Wikidata_classification(object):
    def  __init__(self, parameters):
        self.parameter_wd = load_parameters('wikidata')
        self.parameter_gr = load_parameters()
        self.folder = self.parameter_wd['output_dir']
        self.id_to_title_instance = dict()

        self.parameters = parameters

    def load_instance_title(self):
        self.id_to_title_instance = load_instance_title(self.parameters)

    def classify_article(self):
        self.load_instance_title()
        ner_mapping = NER_mapping(self.parameters)
        id_to_nerClass = dict()
        personal_titles = set()
        nationalities = set()
        calendar = ner_mapping.get_calendar_months()
        counter = 0
        for id_, (title,instances) in tqdm(self.id_to_title_instance.items()):
            counter +=1
            ner_class, instances, is_nationality = ner_mapping.classify_entity_by_instances(title, instances)
            if is_list_of(title):
                continue
            if ner_class == "TITLE":
                personal_titles.add(id_)
                continue
            if ner_class == "CALENDAR":
                calendar.add(id_)
                continue
            if is_nationality:
                nationalities.add(id_)
                continue
            if ner_class:
                id_to_nerClass[id_] = ner_class
            elif ner_mapping.is_loc(id_):
                id_to_nerClass[id_] = "LOC"
            elif ner_mapping.is_org(id_):
                id_to_nerClass[id_] = "ORG"
            elif ner_mapping.is_misc(id_):
                id_to_nerClass[id_] = "MISC"

            if self.parameters['debug'] and counter >= 1000000:
                break
        pickle_data(id_to_nerClass, self.parameters['wd_to_NER'])
        pickle_data(personal_titles, os.path.join(self.parameters['wikidataNER_folder'], 'personal_titles_wdID.p'))
        pickle_data(nationalities, os.path.join(self.parameters['wikidataNER_folder'], 'nationalities_wdID.p'))
        pickle_data(calendar, os.path.join(self.parameters['wikidataNER_folder'], 'calendar_wdID.p'))


        #convert_wp_to_(self.parameter_wd['wd_to_NER'], "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
        #convert_wp_to_(self.parameter_wd['wd_to_NER'], os.path.join(self.parameters["wikidataNER_folder"], self.parameters["language"], "wp_to_ner_by_title.p"))

    def get_subset_of_wikidata(self, list_of_id, name):
        file_path = os.path.join("/dlabdata1/braemy/wikidata-a_2_classification/", name + ".p")
        # self.build_mapping(list_of_id, file_path)

        self.load_instance_title()
        wd_id_of_subclasses = load_pickle(file_path)
        wd_set_output = set()
        for id_, instances in tqdm(self.id_to_instance.items()):
            for instance in instances:
                if instance in wd_id_of_subclasses:
                    wd_set_output.add(id_)
        convert_wd_id_to_wp_title(wd_set_output,
                                  "/dlabdata1/braemy/wikipedia_classification/wp_by_title_" + name + ".p")

