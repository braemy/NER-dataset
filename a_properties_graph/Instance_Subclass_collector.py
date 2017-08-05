# -*- coding: utf-8 -*-

import logging
import os
import time

import boto.s3.connection
import lxml.etree as ET

from properties_graph.Hierarchy import Hierarchy
from properties_graph.Wikidata_entry import Wikidata_entry
from utils import *


class Instance_subclass_collector(object):

    @staticmethod
    def collect_instance_and_sublass():
        parameters = load_parameters('wikidata')
        conn = boto.connect_s3(
            anon=True,
            host='datasets.iccluster.epfl.ch',
            # is_secure=False,               # uncomment if you are not using ssl
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )
        bucket = conn.get_bucket("wikidatawiki")
        key = bucket.get_key('wikidatawiki-20170301-pages-articles-multistream.xml')
        iterator = ET.iterparse(key, encoding="utf-8", recover=True)

        start = time.time()
        wiki = "{http://www.mediawiki.org/xml/export-0.10/}"
        extract = False
        iter_ = 0

        output_dir = parameters['output_dir_collect_instance_subclass']
        create_folder(output_dir)
        output_dir = os.path.join(output_dir, 'data' + get_current_time_in_miliseconds())
        create_folder(output_dir)

        hierarchy = Hierarchy(output_dir)
        print(output_dir)
        logging.basicConfig(filename=os.path.join(output_dir, "error.log"), level=logging.INFO, filemode='w')
        log = logging.getLogger("ex")
        print("Start collecting instances and subclasses...")
        for event, elem in iterator:
            try:
                tag = elem.tag
                value = elem.text
                if tag == wiki + "title" and value is not None and value.startswith("Q"):
                    extract = True
                    iter_ += 1
                    if iter_ % 100000 == 0:
                        print(iter_, 'Execution time: ', (time.time() - start) / 60, "minutes")
                    #if iter_ % 1000000 == 0:
                    #    hierarchy.save_dicts()
                if tag == wiki + "text" and extract:
                    extract = False
                    content = elem.text
                    if content is not None:
                        wikidata_entry = Wikidata_entry(content)
                        id_, title = wikidata_entry.getTitle()
                        if title is None:
                            continue
                        hierarchy.add_id_title(id_, title)
                        instance_of_list, subclass_of_list = wikidata_entry.extract_instance_subclass()
                        hierarchy.add_instance_subclass(id_, title, instance_of_list, subclass_of_list)
                elem.clear()
            except:
                print("Error")
                log.exception("New error at entity:")
                log.exception(content)
                continue

        hierarchy.save_dicts()

        end = time.time()
        print('Execution time: ', end - start)


