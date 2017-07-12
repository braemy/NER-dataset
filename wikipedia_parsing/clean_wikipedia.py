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


def main(id_max, subpart=None):
    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/wiki_text.py")
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/Trie.py")
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/alternative_titles.py")
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/wiki_cleaners.py")
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/wiki_infos.py")
    sc.addPyFile("/home/braemy/NER-dataset/wikipedia_parsing/wiki_replacers.py")


    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    wp_to_ner_by_title = load_pickle("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")


    if subpart is not None:
        sport_set = load_pickle("/dlabdata1/braemy/wikipedia_classification/wp_by_title_"+subpart+".p")
        output_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+subpart+".parquet"
    else:
        output_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+".parquet"
    input_filename = "hdfs:///user/braemy/enwiki.parquet"
    wikipediaDf = sqlContext.read.parquet(input_filename)

    if id_max >= 0:
        wikipediaDf = wikipediaDf.filter(wikipediaDf['id']<id_max)

    if subpart is not None:
        wikipediaDf = wikipediaDf.filter(wikipediaDf['title'].isin(sport_set))


    wikipediaDf = wikipediaDf.map(lambda r: Wiki_text(r,cleaning=True).clean(wp_to_ner_by_title)) # wikipedia_to_wikidata, wikidata_to_ner,wikiTitle_to_id

    wiki_parsed = sqlContext.createDataFrame(wikipediaDf)

    #wiki_parsed.map(lambda r: json.dumps({'title': r.title, 'text': r.text, 'links':r.links})).saveAsTextFile("hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+".json")
    wiki_parsed.write.parquet(output_filename)

    output_filename = "hdfs:///user/braemy/alternative_titles2.json"
    #alternative_titles = wikipediaDf.flatMap(lambda r: r.alt).filter(lambda x: len(x)>0).reduceByKey(lambda a, b: a + b)
    #alternative_titles = sqlContext.createDataFrame(alternative_titles)
    #alternative_titles.map(lambda r: json.dumps(r)).saveAsTextFile(output_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id_max', type=int,   help='maximum id to process', default=-1)
    parser.add_argument('--subpart', type=str,   help='parse only subpart', default=None, required=False)

    args = parser.parse_args()
    main(args.id_max, args.subpart)





