import sys
from pprint import pprint

sys.path.append("..")
import string
import utils
import constants as constants
from collections import Counter

def personal_titles():
    with open("/dlabdata1/braemy/CoNLL/eng-simplified.train", "r") as file:
        before_per = []
        previous_token = ""
        previous_label = ""
        position = 0
        for line in file:
            line_list = line.split()

            if position!=1 and len(line_list) == 3 and  line_list[2] == "B-PER":
                if previous_token != "" and previous_token not in string.punctuation and previous_token[0].isupper():
                    if previous_token in constants.NOT_TITLE or previous_label == "B-MISC" or utils.is_date(previous_token): # => adjectif like American
                        print(line, previous_token, previous_label)
                    else:
                        before_per.append(previous_token)
            if not line_list:
                position = 0
            else:
                position += 1
            previous_token = line_list[0] if len(line_list) == 3 else ""
            previous_label = line_list[2] if len(line_list) == 3 else ""

        #keep only token that appears at least k times
        set_before_per = {k for k,v in Counter(before_per).items() if v > 1}
        print(set_before_per)
        utils.pickle_data(set_before_per, "/dlabdata1/braemy/CoNLL/personal_titles.p")

def sentence_starter():
    with open("/dlabdata1/braemy/CoNLL/eng-simplified.train", "r") as file:
        starter = dict()
        start = True
        for line in file:
            tokens = line.split()
            if tokens:
                if start:
                    word = tokens[0]
                    label = tokens[2]
                    if word.isdigit() or not word.istitle() or word.islower() or label != "O":
                        start = False
                        continue
                    if word in string.punctuation:
                        continue
                    start = False
                    if word not in starter:
                        starter[word] = {'start':1, 'inside':0}
                    else:
                        starter[word]['start'] += 1
            else:
                start = True

        start = True
        for line in file:
            tokens = line.split()
            if tokens:
                if start:
                    start = False
                    continue
                word = tokens[0]
                if word in starter:
                    starter[word]['inside'] +=1
            else:
                start = True

        starter = {k.title() for k,v in starter.items() if v['inside']< 10 and v['start'] > 10}
        utils.pickle_data(starter, "/dlabdata1/braemy/CoNLL/sentence_starter.p")

if __name__ == '__main__':
    sentence_starter()