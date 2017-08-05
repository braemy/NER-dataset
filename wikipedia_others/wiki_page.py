from c_wikipedia_preprocessing import Wikipedia_cleaner


class Wiki_page(object):
    def __init__(self, title, id, text):
        self.title = title
        if title is None:
            self.title = "NO-TITLE"
        self.id = id
        p = {'title': title, 'id': id, 'text': text}
        self.text = Wikipedia_cleaner(p)

    def __str__(self):
        return self.title + " (" + self.id + ")"

    def is_redirect(self):
        return self.text.is_redirect_()

    #def print(self):
    #    self.text.print()

    def parse(self, wp_to_ner_by_title=None,personal_titles=None, alternative_titles=None, sentence_starter=None):
        p = {'title': self.title, 'id': id, 'text': self.cleaned, 'links':self.links}
        parser =  Wikipedia_parser(p, "wp2", personal_titles=personal_titles, sentence_starter=sentence_starter, alternative_titles=alternative_titles)
        self.parsed = parser.parse(wp_to_ner_by_title)
        return self.parsed

    def get_text(self):
        return self.text.text

    def get_content(self):
        return self.parsed

    def clean(self, wp_to_ner_by_title):
        self.cleaned, self.links = self.text.clean_localy(wp_to_ner_by_title)
        return self.cleaned, self.links
