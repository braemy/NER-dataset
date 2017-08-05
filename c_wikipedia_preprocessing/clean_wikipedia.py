# -*- coding: utf-8 -*-
import argparse
import os
from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

sys.path.append(".")
sys.path.append("../../")
from utils import *
#from wikipedia_others.c_wikipedia_preprocessing.wikipedia_cleaner import Wikipedia_cleaner
from wikipedia_cleaner import Wikipedia_cleaner



def main(args):
    parameters = load_parameters_dataset_builder(args.language, "parameters.yml")
    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/c_wikipedia_preprocessing/wikipedia_cleaner.py")
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")
    sc.addPyFile("/home/braemy/NER-dataset/c_wikipedia_preprocessing/alternative_titles.py")
    sc.addPyFile("/home/braemy/NER-dataset/c_wikipedia_preprocessing/helper_remove.py")
    sc.addPyFile("/home/braemy/NER-dataset//c_wikipedia_preprocessing/wiki_infos.py")
    sc.addPyFile("/home/braemy/NER-dataset//c_wikipedia_preprocessing/helper_replace.py")


    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    wp_to_ner_by_title = load_pickle("{0}/{1}/wp_to_ner_by_title.p".format(
        parameters['wikidataNER_folder'],
        parameters['language'],
    ))
    folder = "hdfs:///user/braemy/"+args.language
    input_filename = os.path.join(folder,args.language+"wiki.parquet")
    output_filename = os.path.join(folder, "link_infos.json")

    wikipediaDf = sqlContext.read.parquet(input_filename)
    wikipediaDf = wikipediaDf.map(lambda r: Wikipedia_cleaner(r).collect_outgoing_link(wp_to_ner_by_title)) # wikipedia_to_wikidata, wikidata_to_ner,wikiTitle_to_id
    wiki_parsed = sqlContext.createDataFrame(wikipediaDf)

    #wiki_parsed.write.parquet(output_filename)
    wiki_parsed.map(lambda r: json.dumps({'title': r.title, 'disamb': r.disamb, 'alt': r.alt,'links': r.links})).saveAsTextFile(output_filename)
    #wiki_parsed.map(lambda r: json.dumps({'title': r.title, 'disamb': r.disamb, 'alt': r.alt})).saveAsTextFile(output_filename)

    #output_filename = "hdfs:///user/braemy/alternative_titles2.json"
    #alternative_titles = wikipediaDf.flatMap(lambda r: r.alt).filter(lambda x: len(x)>0).reduceByKey(lambda a, b: a + b)
    #alternative_titles = sqlContext.createDataFrame(alternative_titles)
    #alternative_titles.map(lambda r: json.dumps(r)).saveAsTextFile(output_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type=str,   help='language', choices=['en', 'de', 'als'])
    args = parser.parse_args()
    main(args)





