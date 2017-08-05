import glob
import json
import os

import pickle

from tqdm import tqdm


def build_mapping(parameters):

    input_file = os.path.join(parameters["wikidataNER_folder"],"wikidata/part-*")
    id_to_instanceOf = dict()
    id_to_sublcassOf = dict()
    id_LOC = dict()
    id_ORG = dict()
    id_to_title = dict()
    for file in tqdm(glob.glob(input_file)):

        with open(file, "r") as file:
            for line in file:
                line = json.loads(line)
                if line["subclassOf"]:
                    id_to_sublcassOf[line["id"]] = line["subclassOf"]
                if line["instanceOf"]:
                    id_to_instanceOf[line["id"]] = line["instanceOf"]
                if line["title"]:
                    id_to_title[line["id"]] = line["title"]
                if line["org"]:
                    id_ORG[line["id"]] =line["org"]

                if line["loc"]:
                    id_LOC[line["id"]] =line["loc"]


    with open(os.path.join(parameters["wikidataNER_folder"],"id_to_subclassOf.p"), "wb") as file:
        pickle.dump(id_to_sublcassOf, file)
    with open(os.path.join(parameters["wikidataNER_folder"],"id_to_instanceOf.p"), "wb") as file:
        pickle.dump(id_to_instanceOf, file)
    with open(os.path.join(parameters["wikidataNER_folder"],"id_to_loc.p"), "wb") as file:
        pickle.dump(id_LOC, file)
    with open(os.path.join(parameters["wikidataNER_folder"],"id_to_org.p"), "wb") as file:
        pickle.dump(id_ORG, file)
    with open(os.path.join(parameters["wikidataNER_folder"],"id_to_title.p"), "wb") as file:
        pickle.dump(id_to_title, file)


if __name__ == '__main__':
    build_mapping()
