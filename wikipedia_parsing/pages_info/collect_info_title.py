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

    return Row(title=line.title, id=line.id, lowercase=is_lowercase(line.text))

def is_lowercase(text):
    return int("{{Lowercase}}" in text)

categories = sqlContext.createDataFrame(wikipediaDf.filter("title is not null").map(get_info))

categories.map(lambda r: json.dumps({'title': r.title, 'id': r.id, 'lc':r.lowercase})).saveAsTextFile("hdfs:///user/braemy/wpTitle_to_wpId.json")

