# -*- coding: utf-8 -*-
from bz2 import BZ2File
from glob import glob

import argparse
import os
import shutil
import subprocess
import sys

from a_properties_graph.build_mapping import build_mapping
from analyse_conll.analyse import *
from b_classification.Wikidata_classification import Wikidata_classification
from b_classification.wikipedia_wikidata import map_wpID_to_wdId, map_wpTitle_to_NER
from script_swiss_german.convert_ortho import convert
from utils import *
from wikipedia_others.convert_to_ner_file import convert_to_ner_file
from wikipedia_others.pages_info.convert_to_dict import convert_to_dict_page_infos
from c_wikipedia_preprocessing.alternative_titles import extend_alternative_titles, build_altenative_mapping, anchor_to_articles, \
    allowed_capitalised

sys.path.append("..")
# make sure pyspark tells workers to use python3 not 2 if both are installed
os.environ['PYSPARK_PYTHON'] = '/home/braemy/.conda/envs/dataset3_5/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/braemy/.conda/envs/dataset3_5/bin/python'

"""
======================
How to build a dataset
======================

    1. Classify wikidata titles
    ---------------------------
        a. for all the wd pages, collect the list of instances and subclasses.
            It is done only once and is common for all the languages.
            !!! it takes 6-8 hours
            TODO migrate the code to spark
        b. Using the b_classification package, classify all the titles in the different class

    2. Map wikipedia titles to its corresponding wikidata page
    ----------------------------------------------------------
        the wikidata mapping is wdid->NER, we want a mapping wp->NER
        a. First we need to know the mapping wpId to wdId (wd= wikipedia, wd = wikidata)
        b. Then the mapping: wpTitle to wpId
        c. Finally put everything together

    3. Clean wikipedia, collect reference
    -----------------------------------
        a. We just want to keep the text. So we remove everything else with regex
        a2. in the same time we collect how a pages is referenced in by other wikipedia pages,
            we will need this information for the next step. Everything is handle by the code that
            clean wikipedia
        b. Build a mapping "True article title" -> "how it is referenced in context". We use the informations
            collected to build that mapping (called alternative titles)

    4. Build the dataset
    --------------------
        We need some external information to build the dataset:
            - b_classification of each wpTitle => done in 1b
            - mapping of alternative titles => done in 3b.
            - list of personal titles (Mr, Dr, President,...): need to be done
            - list of word starter: need to be done
        a1. Collect a list of personal titles (cf wikiNER thesis) (word that appears X times before a name)from an other dataset
            we use conll
        a2. Do the same for list of sentence starter: word that appear at the beginning of a sentence
            with capitalized letter and as well in the middle of a sentence without capitalize letter
        b We cab start building the dataset. To build the dataset, we need 3 argument (on top of the language already given)
                - method: wp0 or wp2
                - subpart (facultativ): key word that need to be present in every title: "national team"
                - filter: percentage of data (already filtered by the subpart arg if present) selected ([0;1])
"""

class Dataset_Builder(object):
    def __init__(self, parameters):
        self.parameters = parameters


    #==============
    #      1
    #==============
    # 1.a


    def collect_instance_subclasses(self):
        hadoop_rm(parameters, "id_to_subclassOf.json")
        hadoop_rm(parameters, "id_to_instanceOf.json")
        hadoop_rm(parameters, "id_to_org.json")
        hadoop_rm(parameters, "id_to_loc.json")
        hadoop_rm(parameters, "id_to_title.json")

        command = "spark-submit  --master yarn --num-executors {0} \
                    --executor-memory 8G \
                    --conf spark.driver.memory=64G \
                    --packages com.databricks:spark-xml_2.10:0.4.1\
                    --conf spark.yarn.executor.memoryOverhead=8192 a_properties_graph/properties_collector.py --language {1}".format(
            self.parameters["num_executors"],
            self.parameters["language"]
        )
        subprocess.call(command, shell=True)

        hadoop_get(parameters, "id_to_subclassOf")
        hadoop_get(parameters, "id_to_instanceOf")
        hadoop_get(parameters, "id_to_loc")
        hadoop_get(parameters, "id_to_org")
        hadoop_get(parameters, "id_to_title")

    # 1.b
    def classify_wikidata_title(self):
        wd_classification = Wikidata_classification(self.parameters)
        wd_classification.classify_article()

    # ==============
    #      2
    # ==============
    # 2.a
    def map_wpId_to_wdId(self):
        map_wpID_to_wdId(self.parameters)

    #2.b
    def map_wpTitle_to_wpid(self):
        # remove the output in hdfs in case it already exists
        command = "hadoop fs -rm -r {0}/{1}/wpTitle_to_wpId.json/".format(
            self.parameters['hadoop_folder'],
            self.parameters['language']
        )
        subprocess.call(command, shell=True)
        command = "spark-submit --master yarn  --num-executors {0} --executor-memory 4G --conf spark.yarn.executor.memoryOverhead=1024 wikipedia_others/pages_info/collect_info_title.py --language {1} --parameters_file {2}".format(
            self.parameters["num_executors"],
            self.parameters['language'],
            "parameters.yml"
        )
        subprocess.call(command, shell=True)

        # remove the output file in the file system
        command = "rm -rf {0}/{1}/wpTitle_to_wpId".format(
            self.parameters['wikidataNER_folder'],
            self.parameters['language']
        )
        subprocess.call(command, shell=True)
        # get the data from hdfs to file system
        command = "hadoop fs -get {0}/{1}/wpTitle_to_wpId.json/ {2}/{3}/wpTitle_to_wpId".format(
            self.parameters['hadoop_folder'],
            self.parameters['language'],
            self.parameters['wikidataNER_folder'],
            self.parameters['language']
        )
        subprocess.call(command, shell=True)
        convert_to_dict_page_infos(self.parameters)


    #2.c
    def map_wpTitle_to_NER(self):
        map_wpTitle_to_NER(self.parameters)

    # ==============
    #      3
    # ==============
    def collect_links(self):
        command = "hadoop fs -rm -r {0}/{1}/link_infos.json".format(
            self.parameters['hadoop_folder'],
            self.parameters['language']
        )
        subprocess.call(command, shell=True)
        command = "spark-submit --master yarn --num-executors {0} --executor-memory 1G --conf \
            spark.yarn.executor.memoryOverhead=1024 c_wikipedia_preprocessing/clean_wikipedia.py --language {1}".format(
            self.parameters["num_executors"],
            self.parameters['language']
        )
        subprocess.call(command, shell=True)

        dir_path = "{0}/{1}/link_infos".format(
            self.parameters['wikidataNER_folder'],
            self.parameters['language']
           )
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        command = "hadoop fs -get {0}/{1}/link_infos.json {2}/{1}/link_infos".format(
            self.parameters['hadoop_folder'],
            self.parameters['language'],
            self.parameters['wikidataNER_folder'],
        )
        subprocess.call(command, shell=True)

    # 3b
    def build_altenative_titles(self):
        build_altenative_mapping(self.parameters)
        #remove_non_entities_using_link(self.parameters)
        #extend_alternative_titles(self.parameters)
        anchor_to_articles(self.parameters)
        allowed_capitalised(self.parameters)

    #3.a
    def clean_wikipedia(self):
        #remove the output in hdfs in case it already exists
        #command = "hadoop fs -rm -r {0}/{1}/wikipedia_cleaned.parquet".format(
        #    self.parameters['hadoop_folder'],
        #    self.parameters['language']
        #)
        #subprocess.call(command, shell=True)
        #command = "spark-submit --master yarn --num-executors 100 --executor-memory 2G --conf \
        #    spark.yarn.executor.memoryOverhead=1024 wikipedia_others/c_wikipedia_preprocessing/clean_wikipedia.py --language {0}".format(
        #    self.parameters['language']
        #)
        #subprocess.call(command, shell=True)
        command = "python wikiextractor/WikiExtractor.py --output {0}/{1}/wiki_cleaned{5} --links --section --lists --json --bytes 100M --processes 100 --compress {4} {2}/{1}/{1}wiki-{3}-pages-articles.xml.bz2".format(
            self.parameters['wikidataNER_folder'],
            self.parameters['language'],
            self.parameters['input_folder'],
            self.parameters['wiki_dump'],
            "--keep_tables" if self.parameters['keep_table'] else "",
            "_with_tables" if self.parameters['keep_table'] else ""
        )
        subprocess.call(command, shell=True)
        outgoingLinks_by_titles = load_json("{0}/{1}/outgoingLinks_by_titles.json".format(
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
                    if line["title"] in outgoingLinks_by_titles:
                        line['out_links'] = outgoingLinks_by_titles[line["title"]]
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
    def collect_info_from_conll(self):
        sentence_starter(self.parameters)
        personal_titles(self.parameters)
        allowed_as_capitalized(self.parameters)

    def build_dataset(self):
        print("Start building the dataset")
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
                      "d_wikipedia_parsing/tokenize_wikipedia.py --language {1} --filter {2} --method {3} --subpart {4}".format(
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
            convert_to_ner_file(self.parameters["language"], self.parameters["method"], self.parameters["filter"], self.parameters["keep_table"], self.parameters['subpart'])

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
                      " d_wikipedia_parsing/tokenize_wikipedia.py --language {1} --filter {2} --method {3}".format(
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
        with open(input_file, "r") as file:
            for line in file:
                line = line.strip()
                if line:
                    line = line.split()
                    token = line[0]
                    pos = line[1]
                    tag = line[2]
                    capi = is_capitalized(token)
                    if token.lower() in als_to_de:
                        new_token = als_to_de[token.lower()]
                        if capi:
                            new_token = new_token.titles()
                    else:
                        new_token = convert(token)
                        if token != new_token:
                            a=1
                    output.append(" ".join([new_token, pos, tag]))
                else:
                    output.append(" ")
        with open(output_file, "w") as file:
            file.write("\n".join(output))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type=str,   help='language',choices=["en", "de", "als"], required=True)

    args = parser.parse_args()

    parameters = load_parameters_dataset_builder(args.language, "parameters.yml")
    pprint(parameters)
    dataset_builder = Dataset_Builder(parameters)

    #dataset_builder.collect_instance_subclasses()
    #dataset_builder.classify_wikidata_title()

    #From wikipedia to NER class
    dataset_builder.map_wpId_to_wdId()
    dataset_builder.map_wpTitle_to_wpid()
    dataset_builder.map_wpTitle_to_NER()

    #dataset_builder.collect_links()
    #dataset_builder.build_altenative_titles()

    #dataset_builder.clean_wikipedia()
    #dataset_builder.collect_info_from_conll()
    #dataset_builder.build_dataset()
    #if args.language == "als":
    #    dataset_builder.apply_swiss_german_rules()