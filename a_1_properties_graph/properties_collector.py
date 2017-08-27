# -*- coding: iso-8859-15 -*-
import argparse
import inspect
import json
import os
import sys


sys.path.append(".")
from utils import load_parameters_dataset_builder, check_hadoop_input_file

from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

#os.environ['PYSPARK_PYTHON'] = '/home/braemy/.conda/envs/dataset3_5/bin/python'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/braemy/.conda/envs/dataset3_5/bin/python'


INSTANCE_OF_ID = "P31"  # that class of which this subject is a particular example and member. (Subject typically an individual member with Proper Name label.)
# Different from P279 (subclass of).
SUBCLASS_OF_ID = "P279"  # all instances of these items are instances of those items; this item is a class (subset) of
# that item. Not to be confused with Property:P31 (instance of).
INDUSTRY = "P452"
COORDINATE = "P625"
HQ_LOCATION = "P159"
SPORT= "P641"

def toJson(r):
    if r['format'] != "application/json":
        return Row(id=None, title= None, instanceOf=None, subclassOf= None, org=None, loc=None, misc=None)
    content = json.loads(r['text'])
    title = getTitle(content)
    instance_of_list, subclass_of_list, org, loc, misc = extract_instance_subclass(content)
    return Row(id=r["id"], title=title, instanceOf=instance_of_list, subclassOf=subclass_of_list, org=org, loc=loc, misc=misc)

def getTitle(line):
    try:
        labels = line['labels']
    except:
        return None
    # try to extract english title:
    try:
        title = labels['en']['value']
        return title
    except:
        if list(labels):
            title = labels.get(list(labels)[0])['value']
            return title
    return None


def concat_claims(claims):
    for rel_id, rel_claims in claims:
        for claim in rel_claims:
            yield claim


def extract_instance_subclass(line):
    if "claims" not in line:
        return [], [], False, False, False
    claims = line['claims']
    instance_of_list = []
    subclass_of_list = []
    org = False
    loc = False
    misc = False
    if INSTANCE_OF_ID in claims:
        for inst in claims[INSTANCE_OF_ID]:
            elem = inst["mainsnak"]
            if "datavalue" in elem:
                elem = elem["datavalue"]
                if "value" in elem:
                    elem = elem["value"]
                    if "id" in elem:
                        instance_of_list.append(elem["id"])
                    elif "numeric-id" in elem:
                        instance_of_list.append("Q" + str(elem["numeric-id"]))
    if SUBCLASS_OF_ID in claims:
        for inst in claims[SUBCLASS_OF_ID]:
            elem = inst["mainsnak"]
            if "datavalue" in elem:
                elem = elem["datavalue"]
                if "value" in elem:
                    elem = elem["value"]
                    if "id" in elem:
                        subclass_of_list.append(elem["id"])
                    elif "numeric-id" in elem:
                        subclass_of_list.append("Q" + str(elem["numeric-id"]))
    if INDUSTRY in claims or HQ_LOCATION in claims:
        org = True
    if COORDINATE in claims:
        loc = True
    if SPORT in claims:
        misc = True

    if not instance_of_list:
        instance_of_list = None
    if not subclass_of_list:
        subclass_of_list = None

    return instance_of_list, subclass_of_list, org, loc, misc

def main(language):
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    parameters = load_parameters_dataset_builder(language, "parameters.yml")
    input_file = os.path.join(parameters["hadoop_folder"],"input/wikidatawiki-{0}-pages-articles.xml.bz2".format(parameters["wikidata_dump"]))
    check_hadoop_input_file(input_file, os.path.join(parameters["input_folder"], "wikidatawiki-{0}-pages-articles.xml.bz2".format(parameters["wikidata_dump"])))

    wikidata = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='page').load(input_file)
    wikidata.printSchema()
    wikidata = wikidata.selectExpr("title as id", "revision.text._VALUE as text", "revision.format as format")
    wikidata.printSchema()
    wikidata = sqlContext.createDataFrame(wikidata.map(lambda r: toJson(r)))
    wikidata.printSchema()
    wikidata.show()

    output_json = os.path.join(parameters["hadoop_folder"],"id_to_subclassOf.json")
    wikidata.select("id", "subclassOf").where(wikidata.subclassOf.isNotNull()).repartition(1).write.json(output_json)

    output_json = os.path.join(parameters["hadoop_folder"], "id_title_instanceOf.json")
    wikidata.select("id", "title", "instanceOf").repartition(1).write.json(output_json)

    output_json = os.path.join(parameters["hadoop_folder"], "id_to_loc.json")
    wikidata.select("id", "loc").where(wikidata.loc).repartition(1).write.json(output_json)

    output_json = os.path.join(parameters["hadoop_folder"], "id_to_org.json")
    wikidata.select("id", "org").where(wikidata.org).repartition(1).write.json(output_json)

    output_json = os.path.join(parameters["hadoop_folder"], "id_to_misc.json")
    wikidata.select("id", "misc").where(wikidata.misc).repartition(1).write.json(output_json)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--language', type=str,   help='language', choices=['en', 'de', 'als'])
    args = parser.parse_args()
    main(args.language)

