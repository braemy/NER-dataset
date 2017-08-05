# -*- coding: utf-8 -*-

import sys

import argparse
import nltk
from pyspark import SparkContext, SQLContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

sys.path.append(".")
from utils import *
from wikipedia_parser import  Wikipedia_parser
nltk.data.path.append('/home/braemy/nltk_data/')

def main(args):

    sc = SparkContext()
    sc.addPyFile("/home/braemy/NER-dataset/d_wikipedia_parsing/tokenize_wikipedia.py")
    sc.addPyFile("/home/braemy/NER-dataset/utils.py")
    sc.addPyFile("/home/braemy/NER-dataset/constants.py")
    sc.addPyFile("/home/braemy/NER-dataset/d_wikipedia_parsing/Trie.py")
    sc.addPyFile("/home/braemy/NER-dataset/d_wikipedia_parsing/wikipedia_parser.py")
    #sc.addPyFile("/home/braemy/NER-dataset/wikipedia_others/alternative_titles.py")
    #sc.addPyFile("/home/braemy/NER-dataset/wikipedia_others/helper_remove.py")
    #sc.addPyFile("/home/braemy/NER-dataset/wikipedia_others/wiki_infos.py")
    #sc.addPyFile("/home/braemy/NER-dataset/wikipedia_others/helper_replace.py")

    parameters = load_parameters_dataset_builder(args.language, "parameters.yml")
    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    folder = parameters["hadoop_folder"]
    input_filename = os.path.join(folder, args.language, "wiki_cleaned{0}/*/*.bz2".format("_with_tables" if parameters['keep_table'] else ""))
    if args.subpart is not None:
        output_filename = os.path.join(folder, args.language, args.method,
                                       "wikipedia_dataset_{0}{1}_{2}.json".format("with_tables_" if parameters['keep_table'] else "", str(args.filter), args.subpart))
    else:
        output_filename = os.path.join(folder, args.language, args.method,
                                       "wikipedia_dataset_{0}{1}.json".format("with_tables_" if parameters['keep_table'] else "", str(args.filter)))

    if args.method == "wp3":
        alternative_titles = load_json(
            os.path.join(parameters["wikidataNER_folder"], args.language, "alternative_titles_extended.json"))
    else:
        alternative_titles = load_json(
            os.path.join(parameters["wikidataNER_folder"], args.language, "alternative_titles.json"))
        # out_links = load_json(os.path.join("/dlabdata1/braemy/wikidataNER", args.language, "titles_from_links.json"))
    #outgoingLinks_by_titles = load_json("/dlabdata1/braemy/wikidataNER/en/outgoingLinks_by_titles.json")
    #outgoingLinks_by_titles = sc.broadcast(outgoingLinks_by_titles)
    personal_titles = load_personal_titles(args.language)
    sentence_starters = load_sentence_starter(args.language)
    if args.language in ["de", "als"]:
        allowed_with_cap = load_pickle(os.path.join(parameters["wikidataNER_folder"], parameters["language"], "allowed_wit_cap.p"))
    else:
        allowed_with_cap = []
    wp_to_ner_by_title = load_pickle(
        os.path.join(parameters["wikidataNER_folder"], args.language, "wp_to_ner_by_title.p"))
    #wp_to_ner_by_title = dict()
    #outgoingLinks_by_titles = sc.broadcast(outgoingLinks_by_titles)
    #outgoingLinks_by_titles = dict()
    #personal_titles = []
    #sentence_starters = []
    #alternative_titles = []
    section_to_remove = load_parameters_dataset_builder(args.language, "parameters.yml")['section_to_remove']
    #wikipediaDf = sc.textFile(input_filename)


    wikipediaDf = sqlContext.read.format("json").json(input_filename)

    if args.subpart:
        print("Filter:", " ".join(args.subpart.split("_")))
        wikipediaDf = wikipediaDf.where(wikipediaDf.title.like('%' + " ".join(args.subpart.split("_")) + '%'))

    wikipediaDf = wikipediaDf.sample(False, args.filter, seed=0)
    wikipediaDf.printSchema()
    wikipediaDf = wikipediaDf.map(
        lambda r: Wikipedia_parser(args.language, r, args.method, wp_to_ner_by_title, alternative_titles, personal_titles,
                                   sentence_starters, section_to_remove, allowed_with_cap).parse_wikiExt())
    wikipediaDf = sqlContext.createDataFrame(wikipediaDf)
    wikipediaDf.printSchema()
    wikipediaDf.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile(output_filename)

















    return 0
    wp_to_ner_by_title = load_pickle(
        os.path.join("/dlabdata1/braemy/wikidataNER", args.language, "wp_to_ner_by_title.p"))

    if args.method == "wp3":
        alternative_titles = load_json(os.path.join("/dlabdata1/braemy/wikidataNER", args.language, "alternative_titles_extended.json"))
    else:
        alternative_titles = load_json(os.path.join("/dlabdata1/braemy/wikidataNER", args.language, "alternative_titles.json"))
    #out_links = load_json(os.path.join("/dlabdata1/braemy/wikidataNER", args.language, "titles_from_links.json"))
    outgoingLinks_by_titles = load_json("/dlabdata1/braemy/wikidataNER/en/outgoingLinks_by_titles.json")
    personal_titles = load_personal_titles(args.language)
    sentence_starters = load_sentence_starter(args.language)

    #if args.subpart:
    #    print("Filter:", " ".join(args.subpart.split("_")))
    #    wikipediaDf = wikipediaDf.where(wikipediaDf.title.like('%' + " ".join(args.subpart.split("_")) + '%')).map(
    #        lambda r: Wikipedia_parser(r, args.method, alternative_titles, personal_titles,
    #                                   sentence_starters).parse_spark(
    #            wp_to_ner_by_title))  # , parser=sentence_spliter,  tokenizer=tokenizer
    #else:
    wikipediaDf = wikipediaDf.map(
        lambda r: Wikipedia_parser(args.language, r, args.method, alternative_titles, personal_titles,
                                   sentence_starters, outgoingLinks_by_titles.get(r['title'], [])).parse_wikiExt(
            wp_to_ner_by_title))  # , parser=sentence_spliter,  tokenizer=tokenizer

    #wikipediaDf = wikipediaDf.map(lambda r: Row(title=r['title'], text="a"))

    wikipediaDf = sqlContext.createDataFrame(wikipediaDf)

    """
    folder = "hdfs:///user/braemy/"
    input_filename = os.path.join(folder, args.language, "wikipedia_cleaned.parquet")
    if args.subpart is not None:
        output_filename = os.path.join(folder, args.language, args.method, "wikipedia_dataset_"+str(args.filter)+"_"+args.subpart+".json")
    else:
        output_filename = os.path.join(folder, args.language, args.method, "wikipedia_dataset_"+str(args.filter)+".json")

    wikipediaDf = sqlContext.read.parquet(input_filename)
    wikipediaDf = wikipediaDf.sample(False,args.filter, seed=0)

    wp_to_ner_by_title = load_pickle(os.path.join("/dlabdata1/braemy/wikidataNER", args.language, "wp_to_ner_by_title.p"))
    alternative_titles = load_json(os.path.join("/dlabdata1/braemy/wikidataNER", args.language, "alternative_titles.json"))
    personal_titles =load_personal_titles(args.language)
    sentence_starters =load_sentence_starter(args.language)
    if args.subpart:
        print("Filter:", " ".join(args.subpart.split("_")))
        wikipediaDf = wikipediaDf.where(wikipediaDf.title.like('%'+" ".join(args.subpart.split("_"))+'%')).map(lambda r: Wikipedia_parser(r, args.method, alternative_titles, personal_titles, sentence_starters).parse_spark(wp_to_ner_by_title)) #, parser=sentence_spliter,  tokenizer=tokenizer
    else:
        wikipediaDf = wikipediaDf.map(lambda r: Wikipedia_parser(args.language, r, args.method, alternative_titles, personal_titles, sentence_starters).parse_spark(wp_to_ner_by_title)) #, parser=sentence_spliter,  tokenizer=tokenizer

    wikipediaDf = sqlContext.createDataFrame(wikipediaDf)
    wikipediaDf.map(lambda r: json.dumps({'title': r.title, 'text': r.text})).saveAsTextFile(output_filename)

    """
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--filter', type=float,   help='ratio of data to select')
    parser.add_argument('--subpart', type=str,   help='parse only subpart', default=None, required=False)
    parser.add_argument('--method', type=str,   help='method to build dataset',choices=["wpb", "wp0", "wp2", "wp3"])
    parser.add_argument('--language', type=str,   help='language',choices=["en", "de", "als"])

    args = parser.parse_args()
    main(args)