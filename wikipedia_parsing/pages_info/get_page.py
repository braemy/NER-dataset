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


page = wikipediaDf.filter(wikipediaDf.title == "Honda in Formula One")

page.map(lambda r: json.dumps({'title': r.title, 'id': r.id, 'text':r.text})).saveAsTextFile("hdfs:///user/braemy/page.json")

