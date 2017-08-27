# -*- coding: utf-8 -*-

import sys

import argparse
import nltk
import regex
import yaml
from pattern.de import parse, split
from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

sys.path.append(".")
from utils import *
from wikipedia_parser import  Wikipedia_parser
nltk.data.path.append('/home/braemy/nltk_data/')

def main(args):
    parameters = load_parameters_dataset_builder(args.language, "parameters.yml")
    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/e_wikipedia_parsing/tokenize_wikipedia.py")
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")
    sc.addPyFile("/home/braemy/NER-dataset/e_wikipedia_parsing/Trie.py")
    sc.addPyFile("/home/braemy/NER-dataset/e_wikipedia_parsing/wikipedia_parser.py")
    sc.addPyFile("/home/braemy/NER-dataset/treetagger/treetagger_python2.py")
    sc.addPyFile("/home/braemy/NER-dataset/treetagger/bin/tree-tagger")

    #sc.addPyFile("/home/braemy/NER-dataset/treetagger.zip")

    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    folder = parameters["hadoop_folder"]
    input_filename = os.path.join(folder, args.language, "wiki_cleaned{0}/*/*.bz2".format("_with_tables" if parameters['keep_table'] else ""))
    if args.subpart is not None:
        output_filename = os.path.join(folder, args.language, args.method,
                                       "wikipedia_dataset_{0}{1}_{2}.json".format("with_tables_" if parameters['keep_table'] else "", str(args.filter), args.subpart))
    else:
        output_filename = os.path.join(folder, args.language, args.method,
                                       "wikipedia_dataset_{0}{1}.json".format("with_tables_" if parameters['keep_table'] else "", str(args.filter)))


    alternative_titles = load_alternative_titles(parameters)
    personal_titles = load_personal_titles(parameters)
    nationalities = load_nationalities_extended(parameters)
    sentence_starters = load_sentence_starter(parameters)
    allowed_with_cap  = load_allowed_with_cap(parameters)
    calendar = load_calendar(parameters)
    wp_to_ner_by_title = load_wp_to_ner_by_title(parameters)
    section_to_remove = parameters['section_to_remove']

    wikipediaDf = sqlContext.read.format("json").json(input_filename)

    if args.subpart:
        print("Filter:", " ".join(args.subpart.split("_")))
        wikipediaDf = wikipediaDf.where(wikipediaDf.title.like('%' + " ".join(args.subpart.split("_")) + '%'))

    wikipediaDf = wikipediaDf.sample(False, args.filter, seed=0)
    wikipediaDf.printSchema()
    wikipediaDf = wikipediaDf.map(
        lambda r: Wikipedia_parser(parameters,args.language, r, args.method, wp_to_ner_by_title, alternative_titles, personal_titles,
                                   sentence_starters, section_to_remove, allowed_with_cap, nationalities, calendar).parse_wikiExt())
    wikipediaDf = sqlContext.createDataFrame(wikipediaDf)
    wikipediaDf.printSchema()
    wikipediaDf.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile(output_filename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--filter', type=float,   help='ratio of data to select')
    parser.add_argument('--subpart', type=str,   help='parse only subpart', default=None, required=False)
    parser.add_argument('--method', type=str,   help='method to build dataset',choices=["wpb", "wp0", "wp2", "wp3"])
    parser.add_argument('--language', type=str,   help='language')

    args = parser.parse_args()
    main(args)