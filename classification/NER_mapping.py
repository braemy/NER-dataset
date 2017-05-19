import os
from  utils import *
import numpy as np

class NER_mapping(object):
    def __init__(self):
        parameters = load_parameters()['wikidata']
        self.PER = load_pickle(os.path.join(parameters['PER']))
        self.LOC = load_pickle(os.path.join(parameters['LOC']))
        self.ORG = load_pickle(os.path.join(parameters['ORG']))
        self.MISC = load_pickle(os.path.join(parameters['MISC']))
        self.fuck_counter = 0
        self.ner_classes = ['PER', 'LOC', 'ORG', 'MISC']

    def get_ner_class(self, instances):
        is_PER = self.is_PER(instances)
        is_LOC = self.is_LOC(instances)
        is_ORG = self.is_ORG(instances)
        is_MISC = self.is_MISC(instances)

        if is_PER:
            return "PER"
        if is_LOC:
            return "LOC"
        if is_ORG:
            return "ORG"
        if is_MISC:
            return "MISC"
        return None
        #boolean = [is_PER, is_LOC, is_ORG, is_MISC]
        #if np.sum(boolean) == 0:
        #    return None
        #if np.sum(boolean) > 1:
        #    if np.sum(boolean) == 2 and is_MISC:
        #        is_MISC = False
        #        boolean = [is_PER, is_LOC, is_ORG, is_MISC]
        #    self.fuck_counter += 1
        #return self.ner_classes[np.argmax(boolean)]

    def is_PER(self, instances):
        return self.in_list(instances, self.PER)

    def is_LOC(self, instances):
        return self.in_list(instances, self.LOC)

    def is_ORG(self, instances):
        return self.in_list(instances, self.ORG)

    def is_MISC(self, instances):
        return self.in_list(instances, self.MISC)

    def in_list(self, to_test, true_list):
        for l in to_test:
            if l in true_list:
                return True
        return False