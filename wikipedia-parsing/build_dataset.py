# -*- coding: utf-8 -*-

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import re
from pyspark import SparkContext, SQLContext
from spacy.en import English
from wiki_text import Wiki_text
from utils import *



sc = SparkContext()
sc.addPyFile("/home/braemy/wikipedia-parsing/wiki_text.py")
sc.addPyFile("/home/braemy/wikipedia-parsing/utils.py")


sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

#input_filename = "hdfs:///datasets/wikipedia/en-oct-2016"
input_filename = "hdfs:///user/braemy/enwiki.parquet"
wikipediaDf = sqlContext.read.parquet(input_filename)

#wikipedia_to_wikidata = load_pickle("/dlabdata1/braemy/wikipedia_classification/wikipedia_to_wikidata.p")
#wikidata_to_ner = load_pickle("/dlabdata1/braemy/wikidata-classification/mapping_wikidata_to_NER.p")
#wikiTitle_to_id = load_pickle("/dlabdata1/braemy/wikipedia_classification/title_to_id_170.p")

#wp_to_ner_by_title = load_pickle_spark("hdfs:///user/braemy/wp_to_ner_by_title.p")
wp_to_ner_by_title = load_pickle("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p")





wikipediaDf.filter("title is not null")
wikipediaDf = wikipediaDf.filter(wikipediaDf['id']<5000).filter( ~wikipediaDf['text'].like("#REDIRECT"))
#wikipediaDf = wikipediaDf.map(lambda r: parse_wikipedia(None, r))
wikipediaDf = wikipediaDf.map(lambda r: Wiki_text(r).parse_spark(wp_to_ner_by_title=wp_to_ner_by_title)) # wikipedia_to_wikidata, wikidata_to_ner,wikiTitle_to_id


#from pyspark.sql.types import *
# all DataFrame rows are StructType
# can create a new StructType with combinations of StructField
#schema = StructType([
#    StructField("title", StringType(), True),
#    StructField("text", StringType(), True),
    # etc.
#])



categories = sqlContext.createDataFrame(wikipediaDf)

categories.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile("hdfs:///user/braemy/wikipedia_dataset.json")









