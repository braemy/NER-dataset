import os
import shutil

import sys

from utils import *


class Pages_manager(object):
    def __init__(self):
        self.title_to_id = dict()
        self.id_to_title = dict()
        self.title_to_content = dict()
        self.page_counter = 0
        self.dict_id = 0
        output_dir = "/dlabdata1/braemy/wikipedia_classification"
        self.output_dir = os.path.join(output_dir, 'data' + get_current_time_in_miliseconds())
        os.mkdir(self.output_dir)
        print('Output folder:', self.output_dir)
        self.start = time.time()
    def add_page(self, page):
        self._add_title_id(page.title, page.id)
        self.add_title_content(page.title, page.get_content())
        self.page_counter += 1
        if self.page_counter % 100 == 0:
            self.save_all()
            print(self.page_counter, "processed in:", time.time()-self.start, "seconds")


    def _add_title_id(self, title, id_):
        self.title_to_id[title] = id_
        self.id_to_title[id_] = title

    def save_all(self):
        self.title_to_id = self._save(self.title_to_id, "title_to_id")
        self.id_to_title = self._save(self.id_to_title, "id_to_title")
        self.title_to_content = self._save(self.title_to_content, "title_to_content")
        self.dict_id += 1

    def _save(self, dict_, file_name):
        file_name = os.path.join(self.output_dir, file_name + "_"+str(self.dict_id) + ".p")
        pickle_data(dict_, file_name)
        return dict()

    def add_title_content(self, title, content):
        self.title_to_content[title] = content


