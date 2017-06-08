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

def main(id_max, subpart=None):

    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/wiki_text.py")
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")


    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    if subpart is not None:
        input_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+subpart+".parquet"
        output_filename = "hdfs:///user/braemy/wikipedia_dataset_"+str(id_max)+subpart+".json"
    else:
        input_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+".parquet"
        output_filename = "hdfs:///user/braemy/wikipedia_dataset_"+str(id_max)+".json"

    wikipediaDf = sqlContext.read.parquet(input_filename)

    wp_to_ner_by_title = load_pickle("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")
    personal_titles = load_personal_titles()

    wikipediaDf = wikipediaDf.map(lambda r: Wiki_text(r, personal_titles).parse_spark(wp_to_ner_by_title)) #, parser=sentence_spliter,  tokenizer=tokenizer

    wiki_parsed = sqlContext.createDataFrame(wikipediaDf)


    wiki_parsed.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile(output_filename)
    #wiki_parsed.write.parquet("hdfs:///user/braemy/wikipedia_dataset_"+str(id_max)+".parquet")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id_max', type=int,   help='maximum id to process')
    parser.add_argument('--subpart', type=str,   help='parse only subpart', default=None, required=False)

    args = parser.parse_args()
    main(args.id_max, args.subpart)