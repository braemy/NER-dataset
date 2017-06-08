import argparse
import json

import regex

from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *



def collect_title(line):
    #[[True title (| how it appears in the text)]]
    #rows = []
    reg = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
    result = regex.finditer(reg, line['text'])
    for r in result:
        true_title = r.group(1)
        alternative_title = r.group(3)
        if alternative_title:
            return Row(alternative=alternative_title, true=true_title)
            #rows.append(Row(alternative=alternative_title, true=true_title))
           #mapping[alternative_title] = true_title
    return Row(alternative=line['title'], true=line['title'])

def main(id_max=-1, subpart=None):

    #if subpart is not None:
    #    input_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+subpart+".parquet"
    #    output_filename = "hdfs:///user/braemy/alternative_titles_"+str(id_max)+subpart+".json"
    #else:
    #    input_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+".parquet"
    #    output_filename = "hdfs:///user/braemy/alternative_titles_"+str(id_max)+".json"

    input_filename = "hdfs:///user/braemy/wikipedia_cleaned_" + str(id_max) + ".parquet"
    output_filename = "hdfs:///user/braemy/alternative_titles_"+str(id_max)+".json"

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    wikipediaDf = sqlContext.read.parquet(input_filename)

    alternative_titles = sqlContext.createDataFrame(wikipediaDf.filter("title is not null").map(collect_title))


    alternative_titles.map(lambda r: json.dumps({'alternative': r.alternative, 'true': r.true})).saveAsTextFile(output_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #parser.add_argument('--id_max', type=int,   help='maximum id to process')
    #parser.add_argument('--subpart', type=str,   help='parse only subpart', default=None, required=False)

    #args = parser.parse_args()
    #main(args.id_max, args.subpart)
    main()