# -*- coding: utf-8 -*-

import sys
import string
import argparse
import nltk
from collections import Counter
from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from operator import add


sys.path.append(".")
from utils import *



def select_sentence_starter(articles):
    nltk.data.path.append('/home/braemy/nltk_data/')
    text = articles['text']
    parsed_data = nltk.sent_tokenize(text)
    first_words = []
    for sentence in parsed_data:
        first = nltk.word_tokenize(sentence)[0]
        if first not in string.punctuation and is_capitalized(first):
            first_words.append(first.lower())
    return first_words


def get_vocabulary(articles):
    nltk.data.path.append('/home/braemy/nltk_data/')
    text = articles['text']
    parsed_data = nltk.sent_tokenize(text)
    vocabulary = []
    for sentence in parsed_data:
        for word in nltk.word_tokenize(sentence):
            if word not in string.punctuation and not is_capitalized(word):
                vocabulary.append(word)
    return vocabulary




def main(args):
    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")

    parameters = load_parameters_dataset_builder(args.language, "parameters.yml")
    sqlContext = SQLContext(sc)

    folder = parameters["hadoop_folder"]
    input_filename = os.path.join(folder, args.language, "wiki_cleaned{0}/*/*.bz2".format("_with_tables" if parameters['keep_table'] else ""))
    wikipediaDf = sqlContext.read.format("json").json(input_filename)

    voc_output = os.path.join(folder, args.language, "vocabulary.json")
    vocabulary = wikipediaDf.flatMap(get_vocabulary)
    vocabulary = vocabulary.map(lambda word: (word, 1)).reduceByKey(add)
    vocabulary = vocabulary.filter(lambda r: r[1]>= 50)
    vocabulary = sqlContext.createDataFrame(vocabulary)
    vocabulary = vocabulary.selectExpr("_1 as id", "_2 as voc")
    vocabulary.repartition(1).write.json(voc_output)

    sent_output = os.path.join(folder, args.language, "sent_starters.json")
    sent_starters = wikipediaDf.flatMap(select_sentence_starter)
    sent_starters = sent_starters.map(lambda word: (word, 1)).reduceByKey(add)
    sent_starters = sent_starters.filter(lambda r: r[1]>=50)
    sent_starters = sqlContext.createDataFrame(sent_starters)
    sent_starters = sent_starters.selectExpr("_1 as id", "_2 as start")
    sent_starters.show()
    sent_starters.repartition(1).write.json(sent_output)

    merged = sent_starters.join(vocabulary, "id", how="inner")
    merged.show()
    merged_output = os.path.join(folder, args.language, "allow_as_sent_starters.json")
    merged.repartition(1).write.json(merged_output)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type=str,   help='language')

    args = parser.parse_args()
    main(args)