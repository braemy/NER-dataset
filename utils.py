import calendar
import codecs
import datetime
import json
import os
import pickle
import time
import locale
import subprocess
import shutil

import yaml
from tqdm import tqdm
from glob import glob

import sys
sys.path.append(".")
import constants

def get_current_milliseconds():
    '''
    http://stackoverflow.com/questions/5998245/get-current-time-in-milliseconds-in-python
    '''
    return(int(round(time.time() * 1000)))


def get_current_time_in_seconds():
    '''
    http://stackoverflow.com/questions/415511/how-to-get-current-time-in-python
    '''
    return(time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()))


def get_current_time_in_miliseconds():
    '''
    http://stackoverflow.com/questions/5998245/get-current-time-in-milliseconds-in-python
    '''
    return(get_current_time_in_seconds() + '-' + str(datetime.datetime.now().microsecond))

def create_folder(output_dir):
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

def load_pickle(name):
    with open(name, "rb") as file:
        file = pickle.load(file)
        print("{} loaded".format(name))
        return file
def pickle_data(data, file_name):
    with open(file_name, "wb") as file:
        pickle.dump(data,file,protocol=2)
    print("{} saved!".format(file_name))

def load_parameters(name=None):
    with open("parameters.yml", 'r') as ymlfile:
        if name:
            return yaml.load(ymlfile)[name]
        else:
            return yaml.load(ymlfile)

def load_parameters_dataset_builder(language, parameters_file_path):
    with codecs.open(parameters_file_path, 'r', encoding="utf-8") as ymlfile:
        para = yaml.load(ymlfile)['dataset_builder']
        if language not in para:
            return None
        para_language = para[language]
        para_language["wikidataNER_folder"] = para['wikidataNER_folder']
        para_language["input_folder"] = para['input_folder']
        para_language["wd_to_NER"] = para['wd_to_NER']
        para_language["hadoop_folder"] = para['hadoop_folder']
        para_language["wikidata_dump"] = para['wikidata_dump']
        para_language["conll_folder"] = para['conll_folder']
        para_language["debug"] = para['debug']
        para_language["token_train"] = para['token_train']
        para_language["token_valid"] = para['token_valid']
        para_language["token_test"] = para['token_test']
        para_language["nltk_folder"] = para['nltk_folder']
        #reformat data
        #para_language["list_of"] = set(para_language['list_of'])

        if "filter" in para_language:
            para_language["filter"] = float(para_language["filter"])
        return para_language

def load_json(file_path):
    with open(file_path, "r") as file:
        file =  json.load(file)
        print("{} loaded".format(file_path))
        return file
def dump_json(data, file_path):
    with open(file_path, "w") as file:
        json.dump(data, file)
    print("{} saved".format(file_path))

def convert_wp_to_(to_convert, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = load_json("/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json")

    # wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-a_2_classification/mapping_wikidata_to_NER.p")
    wikidata_to_ner = load_pickle(to_convert)
    wp_to_ner_by_title = dict()
    for k_title, infos in tqdm(wikiTitle_to_id.items()):
        try:
            wp_to_ner_by_title[k_title] = {'ner':wikidata_to_ner[wikipedia_to_wikidata[infos['id']]], 'lc': infos['lc']}
        except KeyError:
            continue

    print("Number of entities:", len(wp_to_ner_by_title))
    # pickle_data(wp_to_ner_by_title, "/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
    pickle_data(wp_to_ner_by_title, output_path)

def convert_wd_id_to_wp_title(wikidata_set, output_path):
    wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
    wikiTitle_to_id = load_json("/dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId.json")

    wikidata_set_title = set()
    for k_title, infos in tqdm(wikiTitle_to_id.items()):
        try:
            if wikipedia_to_wikidata[str(infos['id'])] in wikidata_set:
                wikidata_set_title.add(k_title)
        except:
            continue
    print("Number of entities:",len(wikidata_set_title))
    pickle_data(wikidata_set_title, output_path)






def load_id_to_title(folder):
    print("Loading id_to_title...")
    return load_pickle(os.path.join(folder, "id_title_dict_0.p"))


def load_title_to_id(folder):
    print("Loading title_to_id...")
    return load_pickle(os.path.join(folder, "title_to_dict_0.p"))

def is_date(token, language):
    main_language = get_main_language(language)
    try:
        c = constants.LANGUAGE_FULL_NAME[main_language]
    except KeyError:
        raise NotImplemented

    locale.setlocale(locale.LC_ALL,c)


    return token.capitalize() in calendar.day_abbr or \
        token.capitalize() in calendar.day_name or \
        token.capitalize() in calendar.month_abbr or \
        token.capitalize() in calendar.month_name or \
        token in ["AD", "PM", "AM", "Century", "Millenium"]




def split_tag(ner_class):
    if ner_class == "O":
        return None, "O"
    return ner_class.split("-")


def is_capitalized(token):
    return token[0].isupper()

def create_directory_if_not_exist(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
def create_hadoop_directory_if_not_exist(directory):
    try:
        command = "hadoop fs -ls {0}".format(directory)
        output = subprocess.check_output(command, shell=True)
        print(output)
    except:
        command = "hadoop fs -mkdir {0}".format(directory)
        subprocess.call(command, shell=True)


def get_mapping_DE_ALS(dir=""):
    ger = dir+"script_swiss_german/word-de.train.de.punc.txt"
    als = dir+"script_swiss_german/word-de.train.gsw.punc.txt"

    with codecs.open(ger, "r", encoding="utf-8") as file:
        ger_data = [g.lstrip(":").strip() for g in file]
    with codecs.open(als, "r", encoding="utf-8") as file:
        als_data = [g.lstrip(":").strip() for g in file]

    mapping = dict()
    for g, a in zip(ger_data, als_data):
        mapping[a] = g

    return mapping

def is_disambiguation(text):
    reg = r"{{(D|d)isambiguation\|.*}}"
    if regex.search(reg, text):
        return True
    else:
        return False

def is_list_of(title):
    for list_of in {"list of", "lists of", "Liste"}:
        if list_of.lower().split() == title.lower().split()[:len(list_of.split())]:
            return True
    return False

def hadoop_rm(parameters, file_name, language=False):
    if language:
        command = "hadoop fs -rm -r {0}/{1}/{2}".format(
            parameters["hadoop_folder"],
            parameters["language"],
            file_name
        )
    else:
        command = "hadoop fs -rm -r {0}/{1}".format(
            parameters["hadoop_folder"],
            file_name
        )
    subprocess.call(command, shell=True)

def hadoop_get_json(parameters, file_name, language=False):
    if language:
        dir_path = "{0}/{1}/{2}".format(
            parameters['wikidataNER_folder'],
            parameters["language"],
            file_name
        )
    else:
        dir_path = "{0}/{1}".format(
            parameters['wikidataNER_folder'],
            file_name
        )
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)

    if language:
        command = "hadoop fs -get {0}/{3}/{2}.json {1}/{3}/{2}".format(
            parameters["hadoop_folder"],
            parameters["wikidataNER_folder"],
            file_name,
            parameters["language"]
        )
    else:
        command = "hadoop fs -get {0}/{2}.json {1}/{2}".format(
            parameters["hadoop_folder"],
            parameters["wikidataNER_folder"],
            file_name
        )
    subprocess.call(command, shell=True)

def print_title(title):
    print("="*len(title))
    print(title)
    print("="*len(title))


#======================
# Load Data
#=======================
def load_personal_titles(parameters):
    language = parameters['language']
    main_language = get_main_language(language)
    personal_titles = load_pickle(os.path.join(parameters["wikidataNER_folder"], main_language, "personal_titles.p"))
    if language != main_language:
        personal_titles.update(load_pickle(os.path.join(parameters["wikidataNER_folder"], language, "personal_titles.p")))
    return personal_titles
def load_nationalities(parameters):
    language = parameters['language']
    main_language = get_main_language(language)
    nationalities = load_pickle(os.path.join(parameters["wikidataNER_folder"], main_language, "nationalities.p"))
    if language != main_language:
        nationalities.update(load_pickle(os.path.join(parameters["wikidataNER_folder"], language, "nationalities.p")))
    return nationalities


def load_nationalities_extended(parameters):
    language = parameters['language']
    main_language = get_main_language(language)
    nationalities_extended = load_pickle(os.path.join(parameters["wikidataNER_folder"], main_language, "nationalities_extended.p"))
    if language != main_language:
        nationalities_extended.update(load_pickle(os.path.join(parameters["wikidataNER_folder"], language, "nationalities_extended.p")))
    return nationalities_extended

def load_wp_to_ner_by_title(parameters):
    return load_pickle(os.path.join(parameters["wikidataNER_folder"], parameters['language'], "wp_to_ner_by_title.p"))
def load_calendar(parameters):
    language = parameters['language']
    main_language = get_main_language(language)
    calendar = load_pickle(os.path.join(parameters["wikidataNER_folder"], main_language, "calendar.p"))
    if language != main_language:
        calendar.update(load_pickle(os.path.join(parameters["wikidataNER_folder"], language, "calendar.p")))
    return calendar

def load_allowed_with_cap(parameters):
    language = parameters['language']
    main_language = get_main_language(language)

    if main_language in ["en", "it", "fr"]:
        return []
    elif main_language in ["de"]:
        allowed_with_cap = load_pickle(os.path.join(parameters["wikidataNER_folder"], main_language, "allowed_with_cap.p"))
        if language != main_language:
            allowed_with_cap.update(load_pickle(os.path.join(parameters["wikidataNER_folder"], language, "allowed_with_cap.p")))
        return allowed_with_cap
    else:
        raise NotImplemented

def load_alternative_titles(parameters):
    if parameters["method"] == "wp3":
        return load_json(
            os.path.join(parameters["wikidataNER_folder"], parameters['language'], "title_to_alternatives_extended.json"))
    else:
        return load_json(
            os.path.join(parameters["wikidataNER_folder"], parameters['language'], "title_to_alternatives.json"))

        #try:
    #    if language == "en":
    #        return title.union(constants.TITLE_ABBRS_EN)
    #    return title
    #except:
    #    return set()

def load_sentence_starter(parameters):
    file_name = os.path.join(parameters["language"], "allow_as_sent_starters")
    print("Loading", file_name)
    file_paths = os.path.join(parameters["wikidataNER_folder"],
                              file_name, "part*")
    sent_starter = set()
    if not file_paths:
        print("ERROR:", "file doesn't not exist or is empty")
    for file_path in glob(file_paths):
        print(file_path)
        with codecs.open(file_path, "r", "utf-8") as file:
            for line in tqdm(file):
                try:
                    line = json.loads(line)
                except:
                    print("ERROR:", line)
                    continue
                sent_starter.add(line["id"])
    return sent_starter
    #try:
    #    starter =  {s.title() for s in load_pickle(os.path.join("/dlabdata1/braemy/wikidataNER", language, "sentence_starter.p"))}
    #    if language == "en":
    #        return starter.union(constants.SENTENCE_STARTER_EN)
    #    return starter
    #except:
    #    return set()

def load_instance_title(parameters):
    file_name = "id_title_instanceOf"
    print("Loading", file_name)
    file_paths = os.path.join(parameters["wikidataNER_folder"],
                              file_name, "part*")
    id_to_title_subclass = dict()
    if not file_paths:
        print("ERROR:", "file doesn't not exist or is empty")

    error = 0
    #count = 0
    for file_path in glob(file_paths):
        print(file_path)
        with codecs.open(file_path, "r", "utf-8") as file:
            for line in tqdm(file):
                #count += 1
                #if count == 100:
                #    return id_to_title_subclass
                if "id" in line:
                    #line = line.replace('\u2028',' ')
                    #line = line.replace('\u2029',' ')
                    try:
                        line = json.loads(line)
                        title = line["title"] if "title" in line else ""
                        instance = line["instanceOf"] if "instanceOf" in line else []
                        id_to_title_subclass[line["id"]] = (title, instance)
                    except ValueError:
                        error+=1
                        print(error, line)
                if parameters['debug'] and len(id_to_title_subclass)> 1000000:
                    return id_to_title_subclass
    print("Number of error: ", error)
    return id_to_title_subclass

def load_subclass(parameters):
    return load_json_by_line(parameters, "id_to_subclassOf", "id", "subclassOf")

def load_id_to_loc(parameters):
    return load_json_by_line(parameters, "id_to_loc", "id", "loc")

def load_id_to_misc(parameters):
    return load_json_by_line(parameters, "id_to_misc", "id", "misc")

def load_id_to_org(parameters):
    return load_json_by_line(parameters, "id_to_org", "id", "org")

def load_wdId_to_wdTitle(parameters):
    return load_json_by_line(parameters, "id_to_title", "id", "title" )

def load_redirect(parameters):
    file_name = os.path.join(parameters["language"],"redirect")
    print("Loading", file_name)
    file_paths = os.path.join(parameters["wikidataNER_folder"],
                              file_name, "part*")
    redirect_to_title = dict()
    if not file_paths:
        print("ERROR:", "file doesn't not exist or is empty")
    for file_path in glob(file_paths):
        print(file_path)
        with codecs.open(file_path, "r", "utf-8") as file:
            for line in tqdm(file):
                try:
                    line = json.loads(line)
                except:
                    print("ERROR:", line)
                    continue
                if "title" in line and "_title" in line:
                    true_title = line["_title"]
                    to_be_redirect = line["title"]
                    redirect_to_title.setdefault(true_title, set()).add(to_be_redirect)
    return redirect_to_title



def load_wpTitle_to_wpId(parameters):
    print("Loading", "wpTitle_to_wpId")
    file_paths = os.path.join(parameters["wikidataNER_folder"],parameters["language"],
                              "wpTitle_to_wpId", "part*")
    mapping = dict()
    for file_path in glob(file_paths):
        print(file_path)
        with codecs.open(file_path, "r", "utf-8") as file:
            for line in tqdm(file):
                line = json.loads(line)
                mapping[line['title']] = {'id': int(line['id']), 'lc':line['lc']}
    return mapping

def load_json_by_line(parameters, file_name, key, value):
    print("Loading", file_name)
    file_paths = os.path.join(parameters["wikidataNER_folder"],
                              file_name, "part*")
    id_to_subclass = dict()
    if not file_paths:
        print("ERROR:", "file doesn't not exist or is empty")
    for file_path in glob(file_paths):
        print(file_path)
        with codecs.open(file_path, "r", "utf-8") as file:
            for line in tqdm(file):
                try:
                    line = json.loads(line)
                except:
                    print("ERROR:", line)
                    continue
                if value in line:
                    v = line[value]
                else:
                    v = []
                id_to_subclass[line[key]] = v
    return id_to_subclass

def is_int(x):
    try:
        int(x)
        if len(str(x).split("."))==1:
            return True
        return False
    except ValueError:
        return False

def get_main_language(language):
    try:
        return constants.MAPPING_LANGUAGE[language]
    except KeyError:
        raise NotImplemented
    #if language in ["en"]:
    #    return "en"
    #if language in ["de", "als", "lb"]:
    #    return "de"
    #if language in ["it", "pms", "lmo", "scn", "vec", "nap", "sc", "co", "rm", "lij", "fur"]:
    #    return "it"
    #else:
    #    raise NotImplemented

def get_full_name_language(language):
    main_language = get_main_language(language)
    try:
        return constants.LANGUAGE_FULL_NAME[main_language]
    except KeyError:
        raise NotImplemented
    #if language in ["en"]:
    #    return "english"
    #if language in ["de", "als", "lb"]:
    #    return "german"
    #if language in ["it", "pms", "lmo", "scn", "vec", "nap", "sc", "co", "rm", "lij", "fur"]:
    #    return "italian"
    #else:
    #    raise NotImplemented


def put_file_on_hadoop(file, destination_dir):
    command = "hadoop fs -put {0} {1}".format(
        file,
        destination_dir
    )
    subprocess.call(command, shell=True)


def check_hadoop_input_file(hadoop_file, local_file):
    """
    check if the file exist in hadoop, if not it put the file from the local system into hadoop file system
    :param hadoop_file: path of the file on hadoop
    :type hadoop_file: str
    :param local_file: path of the file on the local file sstem
    :type local_file: st

    """
    print(hadoop_file)
    try:
        command = "hadoop fs -ls {0}".format(hadoop_file)
        results = subprocess.check_output(command, shell=True)
        print(results)
    except:
        #hadoop_directory = os.path.dirname(hadoop_file)
        put_file_on_hadoop(local_file, hadoop_file)


