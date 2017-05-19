import json
from wiki_text import Wiki_text
import sys


class Wiki_page(object):
    def __init__(self, title, id, text):
        self.title = title
        if title is None:
            self.title = "NO-TITLE"
        self.id = id
        p = {'title': title, 'id': id, 'text': text}
        self.text = Wiki_text(p, load_parser=False)

    def __str__(self):
        return self.title + " (" + self.id + ")"

    def is_redirect(self):
        return self.text.is_redirect_()

    def print(self):
        self.text.print()

    def parse(self, parser,wp_to_ner_by_title=None, wikipedia_to_wikidata=None, wikidata_to_ner=None,wikiTitle_to_id=None):
        self.text.parse(parser, wp_to_ner_by_title)

    def get_content(self):
        return self.text.parsed_text