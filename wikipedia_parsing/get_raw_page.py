# -*- coding: utf-8 -*-
import argparse

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import re
from pyspark import SparkContext, SQLContext
from spacy.en import English
from wiki_text import Wiki_text
from utils import *


def main(title):
    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/wiki_text.py")
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/Trie.py")
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")



    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")



    output_filename = "hdfs:///user/braemy/raw_page_"+title+".json"
    input_filename = "hdfs:///user/braemy/enwiki.parquet"
    wikipediaDf = sqlContext.read.parquet(input_filename)

    wikipediaDf = wikipediaDf.where(wikipediaDf.title.like('%' + " ".join(title.split("_")) + '%'))

    wikipediaDf.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile(output_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', type=str,   help='parse only subpart', default=None, required=True)

    args = parser.parse_args()
    main(args.title)





