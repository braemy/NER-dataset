import os

from utils import *

pedia_to_data = dict()
data_to_pedia = dict()
folder = "/dlabdata1/braemy/data"
file_name = "enwiki-20170301-wbc_entity_usage.sql"


with open(os.path.join(folder, file_name), "r") as file:
    data = file.read()
    print("done")