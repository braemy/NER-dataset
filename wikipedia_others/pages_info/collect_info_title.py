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
from utils import load_parameters_dataset_builder



def collect_info_title(args):
    parameters = load_parameters_dataset_builder(args.language, args.parameters_file)

    #input_filename = "hdfs:///user/braemy/enwiki.parquet"
    input_filename = os.path.join(
        parameters['hadoop_folder'],
        parameters['language'],
        "{0}wiki.parquet".format(parameters['language'])
    )
    #output_filename = "hdfs:///user/braemy/wpTitle_to_wpId.json"
    output_filename = os.path.join(
        parameters['hadoop_folder'],
        parameters['language'],
        "wpTitle_to_wpId.json"
    )

    def get_info(line):
        #wd_id = get_wd_id(line['text'])
        #return Row(title=line.title, id=line.id, lowercase=is_lowercase(line.text), wd=wd_id)
        return Row(title=line.title, id=line.id, lowercase=is_lowercase(line.text))

    """
    def get_wd_id(text):
        wd_regex = r"\{\{Sister project links.*d=(.*)\}\}"
        result = regex.search(wd_regex, text)
        if result:
            return result.group(1)
        else:
            return -1
    """
    def is_lowercase(text):
        return int("{{Lowercase}}" in text)

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    wikipediaDf = sqlContext.read.parquet(input_filename)

    categories = sqlContext.createDataFrame(wikipediaDf.filter("title is not null").map(get_info))
    categories.map(lambda r: json.dumps({'title': r.title, 'id': int(r.id), 'lc':r.lowercase})).saveAsTextFile(output_filename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type=str, help='language', choices=["en", "de", "als"])
    parser.add_argument('--parameters_file', type=str, help='parameters file path')

    args = parser.parse_args()
    collect_info_title(args)