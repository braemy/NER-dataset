# -*- coding: utf-8 -*-
import os
import argparse
import json

from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
sys.path.append(".")
from utils import load_parameters_dataset_builder, check_hadoop_input_file



def collect_info_title(args):
    parameters = load_parameters_dataset_builder(args.language, args.parameters_file)
    input_filename = os.path.join(
        parameters['hadoop_folder'],
        parameters['language'],
        "{0}wiki-{1}-pages-articles.xml.bz2".format(parameters['language'], parameters['wiki_dump'])
    )

    check_hadoop_input_file(input_filename,os.path.join(parameters['input_folder'], parameters['language'], "{0}wiki-{1}-pages-articles.xml.bz2".format(parameters['language'], parameters['wiki_dump'])) )
    output_filename = os.path.join(
        parameters['hadoop_folder'],
        parameters['language'],
        "wpTitle_to_wpId.json"
    )

    def get_info(line):
        return Row(title=line.title, id=line.id, lc=is_lowercase(line.revision.text._VALUE))

    def is_lowercase(text):
        if text:
            return int("{{Lowercase}}" in text)
        else:
            return int(False)

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    wikipedia = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page').load(input_filename)
    articles = wikipedia.where("ns = 0").where("redirect is null")
    #articles.printSchema()
    #articles.show()
    #sys.exit()
    categories = sqlContext.createDataFrame(articles.filter("title is not null").map(get_info))
    categories.repartition(1).write.json(output_filename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type=str, help='language')
    parser.add_argument('--parameters_file', type=str, help='parameters file path')

    args = parser.parse_args()
    collect_info_title(args)