# -*- coding: utf-8 -*-
from pprint import pprint

import argparse
import os
import random
import shutil
import subprocess
import sys
from bz2 import BZ2File
from glob import glob

from a_2_classification.Wikidata_classification import Wikidata_classification
from b_mapping_wp_wd.wikipedia_wikidata import map_wpID_to_wdId, map_wpTitle_to_NER
from c_wikipedia_preprocessing.alternative_titles import extend_alternative_titles, build_altenative_mapping, allowed_capitalised
from e_wikipedia_parsing.split_data import split_train_valid_test
from d_analyse_text.analyse import *
from script_swiss_german.convert_ortho import convert
from utils import *
from wikipedia_others.convert_to_ner_file import convert_to_ner_file
from wikipedia_others.pages_info.convert_to_dict import convert_to_dict_page_infos

sys.path.append("..")
# make sure pyspark tells workers to use python3 not 2 if both are installed
#os.environ['PYSPARK_PYTHON'] = '/home/braemy/.conda/envs/dataset3_5/bin/python'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/braemy/.conda/envs/dataset3_5/bin/python'
os.environ['PYTHONHASHSEED'] = '123456'
os.environ['TREETAGGER_HOME'] = 'treetagger/cmd/'
os.environ['TREETAGGER_HOME'] = 'treetagger/bin/'

"""
======================
How to build a dataset
======================

    A. Classify wikidata titles
    ---------------------------
        1. for all the wd pages, collect the list of instances and subclasses.
            It is done only once and is common for all the languages.
            !!! it takes 6-8 hours
            TODO migrate the code to spark
        2. Using the a_2_classification package, classify all the titles in the different class

    B. Map wikipedia titles to its corresponding wikidata page
    ----------------------------------------------------------
        the wikidata mapping is wdid->NER, we want a mapping wp->NER
        1. First we need to know the mapping wpId to wdId (wd= wikipedia, wd = wikidata)
        2. Then the mapping: wpTitle to wpId
        3. Finally put everything together

    C. Clean wikipedia, collect reference
    -----------------------------------
        1. We just want to keep the text. So we remove everything else with regex
        1.2. in the same time we collect how a pages is referenced in by other wikipedia pages,
            we will need this information for the next step. Everything is handle by the code that
            clean wikipedia
        2. Build a mapping "True article title" -> "how it is referenced in context". We use the informations
            collected to build that mapping (called alternative titles)

    D. Build the dataset
    --------------------
        We need some external information to build the dataset:
            - a_2_classification of each wpTitle => done in 1b
            - mapping of alternative titles => done in 3b.
            - list of personal titles (Mr, Dr, President,...): need to be done
            - list of word starter: need to be done
        1.1. Collect a list of personal titles (cf wikiNER thesis) (word that appears X times before a name)from an other dataset
            we use conll
        1.2. Do the same for list of sentence starter: word that appear at the beginning of a sentence
            with capitalized letter and as well in the middle of a sentence without capitalize letter
        2 We cab start building the dataset. To build the dataset, we need 3 argument (on top of the language already given)
                - method: wp0 or wp2
                - subpart (facultativ): key word that need to be present in every title: "national team"
                - filter: percentage of data (already filtered by the subpart arg if present) selected ([0;1])
"""

class Dataset_Builder(object):
    def __init__(self, parameters):
        self.parameters = parameters
        self.language = parameters["language"]
        self.wikidataNER_folder = parameters["wikidataNER_folder"]
        self.hadoop_folder = parameters["hadoop_folder"]
        self.method = parameters["method"]
        self.create_directories()


    def create_directories(self):
        create_directory_if_not_exist(self.wikidataNER_folder)
        create_directory_if_not_exist(os.path.join(self.wikidataNER_folder,self.language))
        create_directory_if_not_exist(os.path.join(self.wikidataNER_folder,self.language, self.method))
        create_hadoop_directory_if_not_exist(os.path.join(self.hadoop_folder, self.language))
        create_hadoop_directory_if_not_exist(os.path.join(self.hadoop_folder, self.language, self.method))

    #==============
    #      A
    #==============
    # 1
    def collect_instance_subclasses(self):
        print_title("Collect instance and subclasses from wikidata")

        hadoop_rm(parameters, "id_to_subclassOf.json")
        hadoop_rm(parameters, "id_title_instanceOf.json")
        hadoop_rm(parameters, "id_to_org.json")
        hadoop_rm(parameters, "id_to_loc.json")
        hadoop_rm(parameters, "id_to_misc.json")

        command = "spark-submit  --master yarn --num-executors {0} \
                    --executor-memory 8G \
                    --conf spark.driver.memory=64G \
                    --packages com.databricks:spark-xml_2.10:0.4.1\
                    --conf spark.yarn.executor.memoryOverhead=8192 a_1_properties_graph/properties_collector.py --language {1}".format(
            self.parameters["num_executors"],
            self.parameters["language"]
        )
        subprocess.call(command, shell=True)

        hadoop_get_json(parameters, "id_to_subclassOf")
        hadoop_get_json(parameters, "id_title_instanceOf")
        hadoop_get_json(parameters, "id_to_loc")
        hadoop_get_json(parameters, "id_to_org")
        hadoop_get_json(parameters, "id_to_misc")


    # 1
    def classify_wikidata_title(self):
        print("Classify wikidata titles")
        wd_classification = Wikidata_classification(self.parameters)
        wd_classification.classify_article()

    # ==============
    #      B
    # ==============
    # 1
    def map_wpId_to_wdId(self):
        print_title("Build the mapping from wikipedia id to wikidata id")
        map_wpID_to_wdId(self.parameters)

    #2
    def map_wpTitle_to_wpid(self):
        print_title("Build the mapping from wikipedia Title to wikipedia ID")
        # remove the output in hdfs in case it already exists
        hadoop_rm(parameters, "wpTitle_to_wpId.json", language=True)

        command = "spark-submit --master yarn \
                  --num-executors {0}  \
                  --executor-memory 4G  \
                  --conf spark.yarn.executor.memoryOverhead=1024 \
                  --packages com.databricks:spark-xml_2.10:0.4.1 \
                  b_mapping_wp_wd/collect_info_title.py --language {1} --parameters_file {2}".format(
            self.parameters["num_executors"],
            self.parameters['language'],
            "parameters.yml"
        )
        subprocess.call(command, shell=True)
        hadoop_get_json(parameters, "wpTitle_to_wpId", language=True)
    #2.c
    def map_wpTitle_to_NER(self):
        print_title("Build the mapping from wikipedia title to NER class")
        map_wpTitle_to_NER(self.parameters)

    # ==============
    #      3
    # ==============
    def collect_links(self):
        print_title("Collect links from Wikipedia")
        hadoop_rm(parameters, "link_infos.json", language=True)
        hadoop_rm(parameters, "redirect.json", language=True)

        command = "hadoop fs -rm -r {0}/{1}/link_infos.json".format(
            self.parameters['hadoop_folder'],
            self.parameters['language']
        )
        subprocess.call(command, shell=True)
        command = "spark-submit --master yarn --num-executors {0} --executor-memory 1G --conf \
            spark.yarn.executor.memoryOverhead=1024  \
            --packages com.databricks:spark-xml_2.10:0.4.1 \
            c_wikipedia_preprocessing/wikipedia_links.py --language {1}".format(
            self.parameters["num_executors"],
            self.parameters['language']
        )
        subprocess.call(command, shell=True)

        hadoop_get_json(parameters, "link_infos", language=True)
        hadoop_get_json(parameters, "redirect", language=True)

    # 3b
    def build_altenative_titles(self):
        print_title("Building alternative titles")
        build_altenative_mapping(self.parameters)
        extend_alternative_titles(self.parameters)

    #3.a
    def clean_wikipedia(self):
        command = "python wikiextractor/WikiExtractor.py --output {0}/{1}/wiki_cleaned{5} --links --section --lists --json --bytes 100M --processes 100 --compress {4} {2}/{1}/{1}wiki-{3}-pages-articles.xml.bz2".format(
            self.parameters['wikidataNER_folder'],
            self.parameters['language'],
            self.parameters['input_folder'],
            self.parameters['wiki_dump'],
            "--keep_tables" if self.parameters['keep_table'] else "",
            "_with_tables" if self.parameters['keep_table'] else ""
        )
        subprocess.call(command, shell=True)

    def put_wiki_clean_on_hadoop(self):
        title_to_outgoingLinks = load_json("{0}/{1}/title_to_outgoingLinks.json".format(
            self.parameters["wikidataNER_folder"],
            self.parameters["language"]
        ))
        input_files = "{0}/{1}/wiki_cleaned{2}/*/*".format(
            self.parameters["wikidataNER_folder"],
            self.parameters["language"],
            "_with_tables" if self.parameters['keep_table'] else ""
        )

        for file_path in tqdm(glob(input_files)):
            output = []
            with BZ2File(file_path, "r") as file:
                for line in file:
                    line = json.loads(line)
                    if line["title"] in title_to_outgoingLinks:
                        line['out_links'] = title_to_outgoingLinks[line["title"]]
                    else:
                        line['out_links'] = []
                    output.append(json.dumps(line))
            with BZ2File(file_path, "w") as file:
                file.write("\n".join(output))
        command = "hadoop fs -rm -r {0}/{1}/wiki_cleaned{2}".format(
            self.parameters['hadoop_folder'],
            self.parameters['language'],
            "_with_tables" if self.parameters['keep_table'] else ""
        )
        subprocess.call(command, shell=True)

        command = "hadoop fs -put {0}/{1}/wiki_cleaned{3} {2}/{1}".format(
            self.parameters['wikidataNER_folder'],
            self.parameters['language'],
            self.parameters['hadoop_folder'],
            "_with_tables" if self.parameters['keep_table'] else ""
        )
        subprocess.call(command, shell=True)


    # ==============
    #      4
    # ==============
    #4a1, a2
    def collect_linguistal_infos(self):
        print_title("Collect info about the language on wikipedia")
        hadoop_rm(parameters, "sent_starters.json", language=True)
        hadoop_rm(parameters, "vocabulary.json", language=True)
        hadoop_rm(parameters, "allow_as_sent_starters.json", language=True)

        command = "spark-submit --master yarn --num-executors {0} --executor-memory 3G --conf spark.driver.memory=64G --conf spark.yarn.executor.memoryOverhead=8192 " \
                  "--conf spark.storage.memoryFraction=0.2  " \
                  "--conf spark.executorEnv.PYTHONHASHSEED=123456 " \
                  "d_analyse_text/analyse_wikipedia.py --language {1}".format(
            self.parameters['num_executors'],
            self.parameters['language'],
        )
        subprocess.call(command, shell=True)
        hadoop_get_json(parameters, "sent_starters", language=True)
        hadoop_get_json(parameters, "vocabulary", language=True)
        hadoop_get_json(parameters, "allow_as_sent_starters", language=True)


    def allowed_as_capitalized(self):
        print_title("Allowed word with capitalized letter")
        allowed_capitalised(self.parameters)

    def build_dataset(self):
        print_title("Start building the dataset")
        if "method" not in self.parameters:
            print("Error if you want to build the dataset, you need to give a 'method' argument {wp0, wp2}")
            sys.exit()
        if "filter" not in self.parameters:
            print("Error if you want to build the dataset, you need to give a 'size' argument [0;1]")
            sys.exit()

        if 'subpart' in self.parameters:
            command = "hadoop fs -rm -r {0}/{1}/{2}/wikipedia_dataset_{5}{3}_{4}.json".format(
                self.parameters['hadoop_folder'],
                self.parameters['language'],
                self.parameters['method'],
                self.parameters['filter'],
                self.parameters['subpart'],
                "with_tables_" if self.parameters['keep_table'] else ""
            )
            subprocess.call(command, shell=True)
            command = "spark-submit --master yarn --num-executors {0} --executor-memory 3G --conf spark.driver.memory=64G --conf spark.yarn.executor.memoryOverhead=8192 " \
                      "--conf spark.storage.memoryFraction=0.2  " \
                      "e_wikipedia_parsing/tokenize_wikipedia.py --language {1} --filter {2} --method {3} --subpart {4}".format(
                self.parameters['num_executors'],
                self.parameters['language'],
                self.parameters['filter'],
                self.parameters['method'],
                self.parameters['subpart'],
            )
            subprocess.call(command, shell=True)

            dir_path = "{0}/{1}/{2}/wikipedia_dataset_{5}{3}_{4}".format(
                self.parameters['wikidataNER_folder'],
                self.parameters['language'],
                self.parameters['filter'],
                self.parameters['method'],
                self.parameters['subpart'],
                "with_tables_" if self.parameters['keep_table'] else ""
            )
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
            command = "hadoop fs -get {0}/{1}/{2}/wikipedia_dataset_{5}{3}_{4}.json/ {5}/{1}/{2}/wikipedia_dataset_{5}{3}_{4}".format(
                self.parameters['hadoop_folder'],
                self.parameters['language'],
                self.parameters['method'],
                self.parameters['filter'],
                self.parameters['subpart'],
                self.parameters['wikidataNER_folder'],
                "with_tables_" if self.parameters['keep_table'] else ""
            )
            subprocess.call(command, shell=True)
            #convert_to_ner_file(self.parameters["language"], self.parameters["method"], self.parameters["filter"], self.parameters["keep_table"], self.parameters['subpart'])

        else:
            command = "hadoop fs -rm -r {0}/{1}/{2}/wikipedia_dataset_{4}{3}.json".format(
                self.parameters['hadoop_folder'],
                self.parameters['language'],
                self.parameters['method'],
                self.parameters['filter'],
                "with_tables_" if self.parameters['keep_table'] else ""
            )
            subprocess.call(command, shell=True)
            command = "spark-submit --master yarn --num-executors {0} --executor-memory 3G --conf spark.driver.memory=64G --conf spark.yarn.executor.memoryOverhead=8192 " \
                      "--conf spark.storage.memoryFraction=0.2  "\
                      " e_wikipedia_parsing/tokenize_wikipedia.py --language {1} --filter {2} --method {3}".format(
                self.parameters['num_executors'],
                self.parameters['language'],
                self.parameters['filter'],
                self.parameters['method'],
            )
            subprocess.call(command, shell=True)

            dir_path = "{0}/{1}/{2}/wikipedia_dataset_{4}{3}".format(
                self.parameters['wikidataNER_folder'],
                self.parameters['language'],
                self.parameters['method'],
                self.parameters['filter'],
                "with_tables_" if self.parameters['keep_table'] else ""
            )
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
            command = "hadoop fs -get {0}/{1}/{2}/wikipedia_dataset_{5}{3}.json/ {4}/{1}/{2}/wikipedia_dataset_{5}{3}".format(
                self.parameters['hadoop_folder'],
                self.parameters['language'],
                self.parameters['method'],
                self.parameters['filter'],
                self.parameters['wikidataNER_folder'],
                "with_tables_" if self.parameters['keep_table'] else ""
            )
            subprocess.call(command, shell=True)
            print("====")
            print("done")
            print("====")
            convert_to_ner_file(self.parameters["language"], self.parameters["method"], self.parameters["filter"], self.parameters['keep_table'])

    def apply_swiss_german_rules(self):
        folder = self.parameters["wikidataNER_folder"]
        method = self.parameters["method"]
        language = self.parameters["language"]
        keep_table = self.parameters["keep_table"]
        id = self.parameters["filter"]
        if "subpart" in self.parameters:
            subpart = self.parameters["subpart"]
            input_file = os.path.join(folder, language, method,
                                       "wikipedia_dataset_{0}{1}_{2}/combined_{3}_{1}_{2}.txt".format(
                                           "with_tables_" if keep_table else "", str(float(id)), subpart, method))
            output_file = os.path.join(folder, language, method,
                                       "wikipedia_dataset_{0}{1}_{2}/combined_processed_{3}_{1}_{2}.txt".format(
                                           "with_tables_" if keep_table else "", str(float(id)), subpart, method))

        else:

            input_file = os.path.join(folder, language, method,
                                       "wikipedia_dataset_{0}{1}/combined_{2}_{1}.txt".format(
                                           "with_tables_" if keep_table else "", str(float(id)), method))
            output_file = os.path.join(folder, language, method,
                                      "wikipedia_dataset_{0}{1}/combined_processed_{2}_{1}.txt".format(
                                          "with_tables_" if keep_table else "", str(float(id)), method))

        als_to_de = get_mapping_DE_ALS()
        output = []
        with codecs.open(input_file, "r", "utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    line = line.split()
                    token = line[0]
                    pos = line[1]
                    tag = line[2]
                    link = " ".join(line[3:]) if len(line) > 3 else ""
                    capi = is_capitalized(token)
                    if token.lower() in als_to_de:
                        new_token = als_to_de[token.lower()]
                        if capi:
                            new_token = new_token.capitalize()
                    else:
                        new_token = convert(token)
                    if len(new_token.split()) > 1:
                        if tag == 'O' or "I-" in tag:
                            for new_t in new_token.split():
                                output.append(" ".join([new_t, pos, tag, link]))

                        else:
                            bio, class_ = tag.split("-")
                            for new_t in new_token.split():
                                output.append(" ".join([new_t, pos, tag, link]))
                                tag = "I-" + class_
                    else:
                        output.append(" ".join([new_token, pos, tag, link]))
                else:
                    output.append(" ")
        with codecs.open(output_file, "w", "utf-8") as file:
            file.write("\n".join(output))

    def split_train_valid_test(self):
        print_title("Split in training, validation and testing")
        split_train_valid_test(self.parameters)
        """
        file_name = "wikipedia_dataset_{0}{1}/combined_{2}_{1}.txt".format(
                                       "with_tables_" if self.parameters['keep_table'] else "", str(float(self.parameters['filter'])), self.parameters['method'])
        input_file = os.path.join(self.parameters['wikidataNER_folder'],
                             self.parameters['language'],
                             self.parameters['method'],
                                   file_name)
        list_sentences = []
        sentence = []
        token_counter = 0
        with codecs.open(input_file,"r", "utf-8") as file:
            for line in file:
                if line.strip():
                    sentence.append(line)
                else:
                    list_sentences.append(sentence)
                    sentence = []
        random.shuffle(list_sentences)

        dataset_types = ['train', 'valid', 'test']
        output_data = []
        token_counter = 0
        dataset_type = dataset_types.pop()
        for sentence in list_sentences:
            token_counter += len(sentence)
            output_data.append("".join(sentence))
            if token_counter >=  self.parameters["token_"+dataset_type]:
                output_file = input_file+"."+dataset_type
                with codecs.open(output_file,"w", "utf-8") as file:
                    for line in output_data:
                        file.write(line)
                        file.write("\n")
                output_data = []
                token_counter = 0
                if dataset_types:
                    dataset_type = dataset_types.pop()
                else:
                    break

"""

    def test(self):
        from pattern.de import parse, split
        from pattern.en import parsetree
        s = parse('Dazu laugfen an deutschen Kliniken mehrere Untersuchungsreihen. Die Katze liegt auf der Matte.',
                      tagset="STTS",
                  tokenize=True,
                  lemmata=True)
        for sentence in split(s):
            output_sentence = []
            for w in sentence.words:
                output_sentence.append((w.string, w.pos, w.lemma))
            print(output_sentence)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #parser.add_argument('--language', type=str,   help='language',choices=["en", "de", "als", 'lb','it', "nds", "ksh", "pfl", "pdc", "fr"], required=False)
    parser.add_argument('--languages', nargs='+', type=str,   help='list of languages to parse', required=True)
    args = parser.parse_args()

    print_title("Start processing multiple languages")
    print(args.languages)
    for language in args.languages:
        print_title("Process {}".format(language))
        parameters = load_parameters_dataset_builder(language, "parameters.yml")
        if not parameters:
            print("Erorr, the language \"{0}\" does not exist".format(language))
        else:
            pprint(parameters)
            dataset_builder = Dataset_Builder(parameters)
            #WIKIDATA
            if parameters['collect_instanceOf_subclassOf']:
                dataset_builder.collect_instance_subclasses()
            if parameters['classify_wikidata_title']:
                dataset_builder.classify_wikidata_title()
            #MAPPING WIKIPEDIA -> WIKIDATA
            if parameters['map_wikipediaId_to_wikidataId']:
                dataset_builder.map_wpId_to_wdId()
            if parameters['map_wpTitle_to_wpId']:
                dataset_builder.map_wpTitle_to_wpid()
            if parameters['map_wpTitle_to_NER']:
                dataset_builder.map_wpTitle_to_NER()

            #CLEAN WIKIPEDIA, COLLECT LINKS
            if parameters['collect_links_from_wikipedia']:
                dataset_builder.collect_links()
            if parameters['build_alternative_titles']:
                dataset_builder.build_altenative_titles()
            if parameters['clean_wikipedia_dump']:
                dataset_builder.clean_wikipedia()
            if parameters['put_wiki_clean_on_hadoop']:
                dataset_builder.put_wiki_clean_on_hadoop()
            if parameters['collect_linguistal_infos']:
                dataset_builder.collect_linguistal_infos()
            if parameters['word_allowed_as_capitalized']:
                dataset_builder.allowed_as_capitalized()

            #
            if parameters['build_NER_dataset']:
                dataset_builder.build_dataset()
            if language == "als":
                dataset_builder.apply_swiss_german_rules()
            if parameters['spit_train_valid_test']:
                dataset_builder.split_train_valid_test()








