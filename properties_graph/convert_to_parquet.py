import json

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
#import json
#from datetime import tzinfo, datetime
#import pytz
from pyspark import SparkContext, SQLContext



input_dump = 'hdfs:///user/braemy/input/wikidata-20170626.json'
output_json = 'hdfs:///user/braemy/wikidata.json'
output_json = 'hdfs:///user/braemy/wikidata_test.json'

sc = SparkContext()

sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

wikidata = sqlContext.read.json(input_dump)


instanceOf_id = "P31"  # that class of which this subject is a particular example and member. (Subject typically an individual member with Proper Name label.)
# Different from P279 (subclass of).
subclassOf_id = "P279"  # all instances of these items are instances of those items; this item is a class (subset) of
# that item. Not to be confused with Property:P31 (instance of).


def getTitle(line):
    try:
        labels = line['labels']
    except KeyError:
        return None
    # try to extract english title:
    try:
        title = labels['en']['value']
        return title
    except KeyError:
        if list(labels):
            title = labels.get(list(labels)[0])['value']
            return title
    return None

def concat_claims(self, claims):
    for rel_id, rel_claims in claims.items():
        for claim in rel_claims:
            yield claim

def extract_instance_subclass(line):
    claims = concat_claims(line['claims'])
    instance_of_list = []
    subclass_of_list = []
    e1 = line['id']
    try:
        for claim in claims:
            mainsnak = claim['mainsnak']
            if mainsnak['snaktype'] != "value":
                continue
            property_ = mainsnak['property']
            if property_ == instanceOf_id:

                try:
                    instance = mainsnak['datavalue']['value']['id']
                except KeyError:
                    instance = "Q" + str(mainsnak['datavalue']['value']['numeric-id'])
                instance_of_list.append(instance)
            if property_ == subclassOf_id:
                try:
                    subclass = mainsnak['datavalue']['value']['id']
                except KeyError:
                    subclass = "Q" + str(mainsnak['datavalue']['value']['numeric-id'])
                subclass_of_list.append(subclass)
    except:
        return instance_of_list, subclass_of_list
    return instance_of_list, subclass_of_list

def get_as_row(line):
    id = line.id
    #title = getTitle(line)
    #instance_of_list, subclass_of_list = extract_instance_subclass(line)

    #return Row(id=id, title=title, instance_of=instance_of_list, subclass_of=subclass_of_list)
    return Row(id=id)

#wk_dataframe = sqlContext.createDataFrame(wikidata.filter(wikidata['id']<"Q1111000000")) #.map(get_as_row)
wk_dataframe = sqlContext.createDataFrame(wikidata.filter(wikidata['id']<"Q222").map(get_as_row))
#wk_dataframe.map(lambda r: json.dumps({'title': r.title, 'id': r.id,
#                                       'instance_of': r.instance_of_list,
#                                        'subclass_of': r.subclass_of_list})
#                 ).saveAsTextFile(output_json)


wk_dataframe.map(lambda r: json.dumps({'id':r.id})
                 ).saveAsTextFile(output_json)