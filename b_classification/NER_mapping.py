import os
from  utils import *
import numpy as np


class NER_mapping(object):
    def __init__(self, parameters):
        self.parameters = parameters
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
                    #'attraction', #attraction
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
        self.LIST_OF = "Q13406463" #Wikimedia list article
        self.DISAMBIGUATION_PAGE = "Q4167410" #Wikimedia disambiguation page
        self.OBJECT = "Q488383" # if we reach this properties, return None

        self.ner_classes = ['PER', 'LOC', 'ORG', 'MISC']
        self.set_up()

    def set_up(self):
        self.id_to_subclass = load_subclass(self.parameters)
        self.id_to_LOC = load_id_to_loc(self.parameters)
        self.id_to_ORG = load_id_to_org(self.parameters)
        print("Set up done")

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
        S.append(source)
        while S:
            v = S.pop()
            if v not in discovered and v not in S:
                discovered.append(v)
                if v in self.id_to_subclass:
                    for n in self.id_to_subclass[v][::-1]:
                        if n != self.OBJECT:
                            S.append(n)
        return discovered

    def is_org(self, id_):
        return id_ in self.id_to_ORG

    def is_loc(self, id_):
        return id_ in self.id_to_LOC

    def classify_entity_by_instances(self, instances):
        if self.LIST_OF in instances or self.DISAMBIGUATION_PAGE in instances:
            return None, []
        for inst in instances:
            v_iterator = self.dfs(inst)
            for o in v_iterator:
                ner_class = self.get_ner_class(o)
                if ner_class:
                    return ner_class, v_iterator
        return None, []


