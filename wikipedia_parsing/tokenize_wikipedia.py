# -*- coding: utf-8 -*-

import json

import nltk
from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from wiki_text import Wiki_text

from utils import *

import argparse


nltk.data.path.append('/home/braemy/nltk_data/')

def main(filter, subpart=None):

    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/wiki_text.py")
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/Trie.py")


    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    if subpart is not None:
        input_filename = "hdfs:///user/braemy/wikipedia_cleaned_-1"+subpart+".parquet"
        output_filename = "hdfs:///user/braemy/wikipedia_dataset_"+str(filter)+subpart+".json"
    else:
        input_filename = "hdfs:///user/braemy/wikipedia_cleaned_-1"".parquet"
        output_filename = "hdfs:///user/braemy/wikipedia_dataset_"+str(filter)+".json"

    wikipediaDf = sqlContext.read.parquet(input_filename)
    wikipediaDf = wikipediaDf.sample(False,filter, seed=0)

    wp_to_ner_by_title = load_pickle("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
    alternative_titles = load_json("/dlabdata1/braemy/wikidataNER/alternative_titles.json")
    personal_titles =load_personal_titles()
    sentence_starters =load_sentence_starter()

    wikipediaDf = wikipediaDf.map(lambda r: Wiki_text(r,args.method, alternative_titles, personal_titles, sentence_starters).parse_spark(wp_to_ner_by_title)) #, parser=sentence_spliter,  tokenizer=tokenizer

    wiki_parsed = sqlContext.createDataFrame(wikipediaDf)


    wiki_parsed.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile(output_filename)
    #wiki_parsed.write.parquet("hdfs:///user/braemy/wikipedia_dataset_"+str(id_max)+".parquet")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--filter', type=float,   help='ratio of data to select')
    parser.add_argument('--subpart', type=str,   help='parse only subpart', default=None, required=False)
    parser.add_argument('--method', type=str,   help='method to build dataset',choices=["wpb", "wp0", "wp2"])

    args = parser.parse_args()
    main(args.filter, args.subpart)