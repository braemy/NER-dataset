# -*- coding: utf-8 -*-

import os
from  utils import *
import numpy as np


class NER_mapping(object):
    def __init__(self, parameters):
        self.parameters = parameters
        self.PER = {'Q215627',  # 'person'
                    'Q95074', # fictional character
                    'Q178885', # deity
                    'Q10856962', #anthroponym
                    'Q21070598', #character that may or may not be fictional
                    'Q10648343', #duo
                    }
        self.ORG = {'Q43229',  # organization'
                    'Q16917',  #hospital
                    'Q37726',  #army
                    'Q215380',  #band
                    'Q167037',  #corporation
                    'Q7188',  #government
                    'Q27686',  #hotel
                    'Q33506',  #museum
                    'Q2566598',  #religious
                    'Q1248784' #airport
                    'Q398141',  #school district
                    'Q1370598',  #place of workship
                    'Q733553',  #channel
                    'Q18127',  #record label
                    'Q11032',  # newspaper
                    'Q16334295',  # group of human
                    'Q874405', #social group
                    'Q131257', #intellectual property
                    #'attraction', #attraction
                    }
        self.LOC = {'Q3257686', #'locality'
                    'Q17334923', #location
                    'Q41176', #building
                    'Q12280', #bridge
                    'Q6999', #atronomical object
                    'Q123705', #neighborhood
                    'Q736917', #geological formation
                    'Q3895768', #fictional location
                    'Q294440', #public space
                    'Q174782', #square
                    }
        self.MISC = {'Q1656682',  # event
                     'Q315',  #language
                     'Q34770',  # language
                     #'Q1190554',  #event
                     'Q198',  # war
                     'Q7748', #law
                     'Q231002',  # nationality
                     'Q9174',  #religion
                     #'Q2424752',  # product
                     'Q482994',  #album
                     'Q7397',  #software
                     'Q728',  #weapon
                     'Q386724',  #work
                     'Q11424', #film
                     'Q7366', #song
                     'Q25379', #play
                     'Q571',  # book

                     'Q31629', # type of sport
                     'Q349', #sport
                     'Q11410', # game
                     'Q483394', #genre
                     'Q41710', #ethnic group
                     'Q11436', #aircraft
                     'Q188889', #code
                     'Q133004', #diaspora
                     'Q618779', #award
                     'Q17444171', #model
                     'Q15056993', #aircraft family
                     'Q19832486', #locomotive class
                     'Q8142', #currency
                     'Q105210', #law
                     #'Q3839081', #disaster
                     #'Q19968906', #broadcasting program
                     #'Q6055843', #internet genre
                     #'Q23807345', #competition
                     #'Q28530532', #type of software
                     #'Q1183543', #device
                     #'Q8242', #litterature
                     'Q17537576', #creative work
                     #'Q431289', #brand
                     #'Q35127', #website
                     #'Q577764', #communication system
                     # 'Q570116', #tourist attraction
                     #'Q16521', #taxon
                     #'Q180684', #conflict
                     #'Q2095', #food
                     #'Q374814', #certification
                     #'Q132241', #festival
                     }
        self.TITLE = {'Q4189293', #title
                      'Q214339', #role
                      'Q216353', # title
                      'Q1326966', #honorific
                      }
        self.GREGORIAN_MONTH = "Q18602249"

        self.LIST_OF = "Q13406463" #Wikimedia list article
        self.DISAMBIGUATION_PAGE = "Q4167410" #Wikimedia disambiguation page
        self.WIKIMEDIA_CAT = "Q4167836"

        self.ner_classes = {'PER', 'LOC', 'ORG', 'MISC'}
        self.set_up()

        self.ORG_KEYWORD = {"schools", "school", "university", "college","museum", "association", "team", "association",\
                            "federation", "inc.", "institute", "brotherhood", "committee", "temple", "party"}
        self.MISC_KEYWORD = {"championship", "game", "currency", "cup", "tour", "trophy", "manga"}

        self.NATIONALITY = {
                        'Q2472587', #people
                        'Q41710', #ethnic group
                        'Q231002', #nationality
                        'Q6266', #nation
        }
        self.CALENDAR = {"Q1790144", #unit of time
                         }

        self.STOP_ITERATION = {"Q16889133", #class
                               "Q5127848", #class
                               "Q217594", #class
                               "Q488383", #object
                               'Q8205328', #artificial physical object
                               }

    def set_up(self):
        self.id_to_subclass = load_subclass(self.parameters)
        self.id_to_LOC = set(load_id_to_loc(self.parameters).keys())
        self.id_to_ORG = set(load_id_to_org(self.parameters).keys())
        self.id_to_MISC = set(load_id_to_misc(self.parameters).keys())
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
        if inst in self.TITLE:
            return "TITLE"
        if inst in self.CALENDAR:
            return "CALENDAR"
        return None

    def dfs(self, source):
        discovered = []
        S = [source]
        while S:
            v = S.pop()
            if v not in discovered and v not in S:
                discovered.append(v)
                if v in self.id_to_subclass:
                    for n in self.id_to_subclass[v][::-1]:
                        if n not in self.STOP_ITERATION:
                            S.append(n)
        return discovered

    def is_org(self, id_):
        return id_ in self.id_to_ORG

    def is_loc(self, id_):
        return id_ in self.id_to_LOC

    def is_misc(self, id_):
        return id_ in self.id_to_MISC

    def is_nationality(self, instances):
        for inst in instances:
            if inst in self.NATIONALITY:
                return True
        return False

    def classify_entity_by_instances(self, title, instances):
        if self.LIST_OF in instances:
            return None, [], None
        is_natio = self.is_nationality(instances)
        if self.DISAMBIGUATION_PAGE in instances:
            return "DISAM", [], is_natio
        if self.WIKIMEDIA_CAT in instances:
            return None, [], is_natio
        list_title_lw = title.lower().split()
        for kw in self.ORG_KEYWORD:
            if kw in list_title_lw:
                return "ORG", [], is_natio
        for kw in self.MISC_KEYWORD:
            if kw in list_title_lw:
                return "MISC", [], is_natio
        for inst in instances:
            v_iterator = self.dfs(inst)
            for o in v_iterator:
                ner_class = self.get_ner_class(o)
                if ner_class:
                    return ner_class, v_iterator, is_natio
        return None, [], is_natio

    def get_calendar_months(self):
        calendar_months = set()
        for id_, subclass in self.id_to_subclass.items():
            if self.GREGORIAN_MONTH in subclass:
                calendar_months.add(id_)
        return calendar_months

