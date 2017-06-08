import string
import utils
with open("../../ner/nerc-conll2003/eng-simplified.train", "r") as file:
    set_before_per = set()
    previous_token = ""
    for line in file:
        line = line.split()
        if len(line) == 3 and  line[2] == "B-PER":
            if previous_token != "" and previous_token not in string.punctuation and previous_token[0].isupper():
                set_before_per.add(previous_token)
        previous_token = line[0] if len(line) == 3 else ""
    print(len(set_before_per))
    print(set_before_per)
    utils.pickle_data(set_before_per, "data/personal_titles.p")
