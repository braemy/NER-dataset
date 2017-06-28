from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
#import json
#from datetime import tzinfo, datetime
#import pytz
from pyspark import SparkContext, SQLContext


input_dump = 'hdfs:///user/braemy/input/enwiki-20170301-pages-articles.xml'
output_parquet = 'hdfs:///user/braemy/enwiki.parquet'

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

wikipedia = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page').load(input_dump)
articles = wikipedia.where("ns = 0").where("redirect is null")

def get_as_row(line):
    return Row(text=line.revision.text._VALUE, id=line.id,
    timestamp=line.revision.timestamp, test=line.revision.id,
    title=line.title)

wk_dataframe = sqlContext.createDataFrame(articles.map(get_as_row))

wk_dataframe.write.parquet(output_parquet)


#spark-submit --master yarn --num-executors 50 --executor-memory 8g --driver-memory 32g --packages com.databricks:spark-xml_2.10:0.4.1  convert_to_parquet.py