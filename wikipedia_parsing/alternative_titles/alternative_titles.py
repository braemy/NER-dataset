import argparse
import json

import pickle

import nltk
import regex

from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

import string



def collect_title(line, wp_to_ner_by_title):
    nltk.data.path.append('/home/braemy/nltk_data/')

    page_title = line['title']

    #redirect page, drop it
    if "#REDIRECT" in line['text']:
        return []
    #if the page is not a Entity => continue
    if page_title not in wp_to_ner_by_title:
        return []
    title_to_altenatives = []
    #Get all link in disambiguation pages
    if "(disambiguation)" in page_title:
        #[[True title (| how it appears in the text)]]
        reg = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
        result = regex.finditer(reg, line['text'])
        for r in result:
            true_title = r.group(1)
            alternative_title = r.group(3) if r.group(3) else true_title
            if "#" not in alternative_title and "#" not in true_title:
                if alternative_title in string.punctuation:
                    continue
                tuple = (true_title, [true_title, alternative_title.strip()])
                if tuple not in title_to_altenatives:
                    title_to_altenatives.append(tuple)
    else:
        #for each page include the title
        alternative_titles = [page_title]

        # the first sentence contains many alternative title in bold. Parse the first sentence to get such title
        sentences = nltk.sent_tokenize(line['text'])
        if not sentences:
            return []
        first_sentence = sentences[0]


        reg = r"\'\'\'([\S\s]*?)\'\'\'"
        reg = regex.compile(reg)
        result = reg.finditer(first_sentence)
        for r in result:
            alternative_titles.append(r.group(1))

        # For each pages being a PER add first and last name as alternative
        #if page_title in wp_to_ner_by_title and wp_to_ner_by_title[page_title]['ner'] == 'PER':
        #    title_splited = page_title.split()
        #    alternative_titles.append(title_splited[0])
        #    alternative_titles.append(title_splited[-1])

        # for each each pages get the redirect as alternative
        redirect_reg1 = r"\{\{Redirect(\d*)\|([^\{\}]*)\}\}"
        alternative_titles = get_redirect(redirect_reg1, line, wp_to_ner_by_title, page_title, alternative_titles)
        redirect_reg2 = r"\{\{redirect-multi\|(\d*)\|([^\{\}]*)\}\}"
        alternative_titles = get_redirect(redirect_reg2, line, wp_to_ner_by_title, page_title, alternative_titles)


        # for each page get the disambiguation that map to that page
        dis_reg = r"\{.*other uses\|([^\{\}]*)\}\}"
        match = regex.search(dis_reg, line['text'])
        if match:
            for disambiguation in match.group(1).split('|'):
                alternative_titles.append(disambiguation)
                if page_title in wp_to_ner_by_title and wp_to_ner_by_title[page_title]['ner'] == 'PER':
                    title_splited = disambiguation.split()
                    alternative_titles.append(title_splited[0])
                    alternative_titles.append(title_splited[-1])
        title_to_altenatives.append((page_title, alternative_titles))
    return title_to_altenatives

def get_redirect(redirect_reg, line, wp_to_ner_by_title, page_title, alternative_titles):
    match = regex.search(redirect_reg, line['text'])
    if match:
        number_of_redirect = int(match.group(1)) if match.group(1) else 1
        for redirect in match.group(2).split('|')[:number_of_redirect]:
            # if the main page is PER => apply the same rule as before to the redirects
            alternative_titles.append(redirect)
            if page_title in wp_to_ner_by_title and wp_to_ner_by_title[page_title]['ner'] == 'PER':
                title_splited = redirect.split()
                alternative_titles.append(title_splited[0])
                alternative_titles.append(title_splited[-1])
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