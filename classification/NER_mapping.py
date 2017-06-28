import os
from  utils import *
import numpy as np


class NER_mapping(object):
    def __init__(self):
        self.parameters = load_parameters()
        parameters_wd = self.parameters['wikidata']
        self.folder = parameters_wd['output_dir']
        self.PER = ['Q215627',  # ['person']
                    'Q95074', # fictional character
                    ]
        self.ORG = ['Q43229',  # organization'
                    'Q16917', #hospital
                    'Q37726', #army
                    'Q215380', #band
                    'Q167037', #corporation
                    'Q7188', #government
                    'Q27686', #hotel
                    'Q33506', #museum
                    'Q2566598', #religious
                    'Q1248784', #airport
                    'attraction', #attraction
                    ]
        self.LOC = ['Q3257686', #'locality'
                    'Q17334923', #location
                    'Q41176', #building
                    'Q12280', #bridge

                    ]
        self.MISC = ['Q1656682',  # event
                     'Q315',  #language
                     'Q34770',  # language

                     'Q1190554',  #event
                     #'Q198',  # war

                     'Q7748', #law

                     'Q231002',  # nationality
                     'Q9174',  #religion

                     'Q2424752',  # product
                     #'Q482994',  #album
                     #'Q7397',  #software
                     #'Q42889',  #vehicle
                     #'Q728',  #weapon

                     'Q386724',  #work
                     #'Q11424', #film
                     #'Q7366', #song
                     #'Q25379', #play
                     #'Q571',  # book

                     ]

        self.fuck_counter = 0
        self.ner_classes = ['PER', 'LOC', 'ORG', 'MISC']
        self.set_up()

    def set_up(self):
        self.load_subclass()
        print("Set up done")

    def load_subclass(self):
        self.id_to_subclass = load_subclass(self.folder)

    def load_instance(self):
        self.id_to_instance = load_instance(self.folder)

    def get_ner_class(self, inst):
        if inst in self.PER:
            return "PER"
        if inst in self.LOC:
            return "LOC"
        if inst in self.ORG:
            return "ORG"
        if inst in self.MISC:
            return "MISC"
        return None

    def dfs(self, source):
        discovered = []
        S = []
        S.append(source)
        while S:
            v = S.pop()
            if v not in discovered and v not in S:
                discovered.append(v)
                if v in self.id_to_subclass:
                    for n in self.id_to_subclass[v][::-1]:
                        # print(n)
                        S.append(n)
        return discovered



    def classify_entity_by_instances(self, instances):
        for inst in instances:
            v_iterator = self.dfs(inst)
            for o in v_iterator:
                ner_class = self.get_ner_class(o)
                if ner_class:
                    return ner_class
        return None


