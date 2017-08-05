# -*- coding: utf-8 -*-
import argparse
import os
import sys

import json
from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *





def main(args):
    sc = SparkContext()

    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    folder = "hdfs:///user/braemy/"
    input_filename = os.path.join(folder, args.language, args.language + "wiki.parquet")
    output_filename = os.path.join(folder, args.language, "raw_page_" + args.title + ".json")

    wikipediaDf = sqlContext.read.parquet(input_filename)
    wikipediaDf = wikipediaDf.where(wikipediaDf.title.like('%' + " ".join(args.title.split("_")) + '%'))
    wikipediaDf.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile(output_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--title', type=str,   help='parse only subpart', default=None, required=True)
    parser.add_argument('--language', type=str,   help='parse only subpart', choices=['en','de'])

    args = parser.parse_args()
    main(args)





