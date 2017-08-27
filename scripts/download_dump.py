import subprocess
import sys
import os
sys.path.append(".")
sys.path.append("..")
from utils import *


def set_up_language(folder, language):
    print("Downloading {}".format(language))
    create_directory_if_not_exist(folder)
    create_directory_if_not_exist(os.path.join(folder, language))
    output_folder = os.path.join(folder, language)
    command = "wget -P {0} https://dumps.wikimedia.org/{1}wiki/20170620/{1}wiki-20170620-pages-articles.xml.bz2".format(output_folder, language)
    subprocess.call(command,shell=True)
    command = "wget -P {0} https://dumps.wikimedia.org/{1}wiki/20170620/{1}wiki-20170620-page_props.sql.gz".format(output_folder, language)
    subprocess.call(command,shell=True)


if __name__ == '__main__':
    #german
    languages_de = ["nds", "li", "ksh", "pfl", "pdc"]
    #french
    languages_fr = ["fr", "oc", "ca", "ht", "wa", "nrm", "pcd", "frp"]
    language_it = ["it", "pms", "lmo", "scn", "vec", "nap", "sc", "co", "rm", "lij", "fur"]

    languages_list = [("Italien",language_it), ("French", languages_fr)]

    folder = "/dlabdata1/braemy/data"
    for l, languages in languages_list:
        print("======")
        print(l)
        print("======")
        for language in languages:
            set_up_language(folder, language)
