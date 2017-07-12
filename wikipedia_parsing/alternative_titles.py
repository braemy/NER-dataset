import argparse
import json

import pickle

import nltk
import regex

#from pyspark import SparkContext, SQLContext
#from pyspark.sql import *
#from pyspark.sql.functions import *
#from pyspark.sql.types import *

import string

def collect_all_outgoing_link(raw_text, wp_to_ner_by_title):
    outgoing_map = dict()
    reg = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
    result = regex.finditer(reg, raw_text)
    for r in result:
        origin_of_the_link = r.group(1) if r.group(3) is None else r.group(3)
        destination_of_the_link = r.group(1)
        if r.group(1) in wp_to_ner_by_title:
            if destination_of_the_link not in outgoing_map:
                outgoing_map[destination_of_the_link] = [origin_of_the_link]
            else:
                outgoing_map[destination_of_the_link].append(origin_of_the_link)

    return outgoing_map

def collect_title_disambiguation(title, text, wp_to_ner_by_title):
    #if the page is not a Entity => continue
    #[[True title (| how it appears in the text)]]
    alternative_title = title.split(" (disambiguation)")[0]
    #get the first bold link:
    title_to_alternatives = dict()
    reg = r"'''\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]'''"
    result = regex.search(reg, text)
    if result and result.group(1) in wp_to_ner_by_title:
        title_to_alternatives[result.group(1)] = [alternative_title.strip("' ")]
    reg = r"\*+ *\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
    result = regex.finditer(reg, text)
    for r in result:
        if r.group(1) in wp_to_ner_by_title:
            title_to_alternatives[r.group(1)] = [alternative_title.strip("' ")]
    return title_to_alternatives

def collect_title_first_paragraph(title, text, wp_to_ner_by_title):
    # the first sentence contains many alternative title in bold. Parse the first sentence to get such title

    # for each page include the title
    if title not in wp_to_ner_by_title:
        return set()
    alternative_titles = set()
    alternative_titles.add(title)
    first_paragraph = text.split("\n\n")[0]
    reg = r"'''([\S\s]*?)'''"
    result = regex.finditer(reg, first_paragraph)
    for r in result:
        alternative_titles.add(r.group(1).strip("' "))
    return alternative_titles


def collect_title_redirect(title, text, wp_to_ner_by_title):
    if title not in wp_to_ner_by_title:
        return set()
    alternative_titles = set()
    alternative_titles.add(title)
    # for each each pages get the redirect as alternative
    redirect_reg1 = r"\{\{Redirect(\d*)\|([^\{\}]*)\}\}"
    alternative_titles = get_redirect(redirect_reg1, text, wp_to_ner_by_title, title, alternative_titles)
    redirect_reg2 = r"\{\{redirect-multi\|(\d*)\|([^\{\}]*)\}\}"
    alternative_titles = get_redirect(redirect_reg2, text, wp_to_ner_by_title, title, alternative_titles)
    return alternative_titles

def get_redirect(redirect_reg, text, wp_to_ner_by_title, title, alternative_titles):
    match = regex.search(redirect_reg, text)
    if match:
        number_of_redirect = int(match.group(1)) if match.group(1) else 1
        for redirect in match.group(2).split('|')[:number_of_redirect]:
            # if the main page is PER => apply the same rule as before to the redirects
            alternative_titles.add(redirect.strip("' "))
            #if title in wp_to_ner_by_title and wp_to_ner_by_title[title]['ner'] == 'PER':
            #    title_splited = redirect.split()
            #    alternative_titles.add(title_splited[0])
            #    alternative_titles.add(title_splited[-1])
    return alternative_titles

def main(id_max=-1, subpart=None):

    #if subpart is not None:
    #    input_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+subpart+".parquet"
    #    output_filename = "hdfs:///user/braemy/alternative_titles_"+str(id_max)+subpart+".json"
    #else:
    #    input_filename = "hdfs:///user/braemy/wikipedia_cleaned_"+str(id_max)+".parquet"
    #    output_filename = "hdfs:///user/braemy/alternative_titles_"+str(id_max)+".json"

    input_filename = "hdfs:///user/braemy/wikipedia_cleaned_-1.parquet"
    output_filename = "hdfs:///user/braemy/alternative_titles.json"

    with open("/dlabdata1/braemy/wikipedia_classification/wp_to_ner_by_title.p", "rb") as file:
        wp_to_ner_by_title = pickle.load(file)

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    wikipediaDf = sqlContext.read.parquet(input_filename)

    alternative_titles = wikipediaDf.filter("title is not null").flatMap(lambda r: collect_title(r, wp_to_ner_by_title)).filter(lambda x: len(x)>0) \
        .reduceByKey(lambda a, b: a + b)
    alternative_titles = sqlContext.createDataFrame(alternative_titles)


    alternative_titles.map(lambda r: json.dumps(r)).saveAsTextFile(output_filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #parser.add_argument('--id_max', type=int,   help='maximum id to process')
    #parser.add_argument('--subpart', type=str,   help='parse only subpart', default=None, required=False)

    #args = parser.parse_args()
    #main(args.id_max, args.subpart)
    main()