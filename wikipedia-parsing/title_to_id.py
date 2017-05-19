# -*- coding: utf-8 -*-

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import re
from pyspark import SparkContext, SQLContext

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

#input_filename = "hdfs:///datasets/wikipedia/en-oct-2016"
input_filename = "hdfs:///user/braemy/enwiki.parquet"
wikipediaDf = sqlContext.read.parquet(input_filename)



#title_to_id = sqlContext.createDataFrame(wikipediaDf.take(10)).map(lambda x: json.dumps({'title': x.title, 'id': x.id})).saveAsTextFile("hdfs:///user/braemy/test.txt")

#title_to_id.map(lambda r: json.dumps({'title': r.title, 'id': r.id})).saveAsTextFile("hdfs:///user/braemy/title_to_id.json")

def get_title_id(line):
    return Row(title=line.title, id=line.id)


categories = sqlContext.createDataFrame(wikipediaDf.filter("title is not null").map(get_title_id))

categories.map(lambda r: json.dumps({'title': r.title, 'id': r.id})).saveAsTextFile("hdfs:///user/braemy/title_to_id.json")









