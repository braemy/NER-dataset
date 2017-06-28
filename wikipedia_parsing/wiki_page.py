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
        self.text = Wiki_text(p, "wp0")

    def __str__(self):
        return self.title + " (" + self.id + ")"

    def is_redirect(self):
        return self.text.is_redirect_()

    #def print(self):
    #    self.text.print()

    def parse(self, wp_to_ner_by_title=None,personal_titles=None, alternative_titles=None):
        self.text.parse(wp_to_ner_by_title,personal_titles=personal_titles, alternative_titles=alternative_titles)

    def get_text(self):
        return self.text.text

    def get_content(self):
        return self.text.parsed_text