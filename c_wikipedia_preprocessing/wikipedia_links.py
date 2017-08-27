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
    sc.addPyFile("/home/braemy/NER-dataset//c_wikipedia_preprocessing/helper_replace.py")

    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    folder = "hdfs:///user/braemy/"+args.language
    input_filename = os.path.join(
        parameters['hadoop_folder'],
        parameters['language'],
        "{0}wiki-{1}-pages-articles.xml.bz2".format(parameters['language'], parameters['wiki_dump'])
    )
    output_filename = os.path.join(folder, "link_infos.json")
    wp_to_ner_by_title = load_wp_to_ner_by_title(parameters)

    wikipedia = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page').load(input_filename)

    redirect = wikipedia.where("ns = 0").where("redirect is not null")
    redirect = redirect.selectExpr("id", "title", "redirect._title")
    redirect.printSchema()
    redirect.show()
    redirect_output = os.path.join(folder, "redirect.json")
    redirect.repartition(1).write.json(redirect_output)


    wikipediaDf = wikipedia.where("ns = 0").where("redirect is null")
    wikipediaDf = wikipediaDf.where("ns = 0").where("revision.text._VALUE is not null")
    wikipediaDf.show()
    wikipediaDf = wikipediaDf.map(lambda r: Wikipedia_cleaner(r, parameters).collect_outgoing_link(wp_to_ner_by_title))
    wiki_parsed = sqlContext.createDataFrame(wikipediaDf, samplingRatio=200)
    wiki_parsed.select("disamb").show(200)
    wiki_parsed.map(lambda r: json.dumps({'title': r.title, 'disamb': r.disamb, 'alt': r.alt,'links': r.links})).saveAsTextFile(output_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type=str,   help='language')
    args = parser.parse_args()
    main(args)





