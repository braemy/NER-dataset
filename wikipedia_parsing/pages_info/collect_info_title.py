# -*- coding: utf-8 -*-

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import re
from pyspark import SparkContext, SQLContext
import regex

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

#input_filename = "hdfs:///datasets/wikipedia/en-oct-2016"
input_filename = "hdfs:///user/braemy/enwiki.parquet"
wikipediaDf = sqlContext.read.parquet(input_filename)


def get_info(line):
    #wd_id = get_wd_id(line['text'])
    #return Row(title=line.title, id=line.id, lowercase=is_lowercase(line.text), wd=wd_id)
    return Row(title=line.title, id=line.id, lowercase=is_lowercase(line.text))

def get_wd_id(text):
    wd_regex = r"\{\{Sister project links.*d=(.*)\}\}"
    result = regex.search(wd_regex, text)
    if result:
        return result.group(1)
    else:
        return -1
def is_lowercase(text):
    return int("{{Lowercase}}" in text)

categories = sqlContext.createDataFrame(wikipediaDf.filter("title is not null").map(get_info))

#categories.map(lambda r: json.dumps({'title': r.title, 'id': r.id, 'lc':r.lowercase, 'wd': r.wd})).saveAsTextFile("hdfs:///user/braemy/wpTitle_to_wpId.json")
categories.map(lambda r: json.dumps({'title': r.title, 'id': int(r.id), 'lc':r.lowercase})).saveAsTextFile("hdfs:///user/braemy/wpTitle_to_wpId.json")

