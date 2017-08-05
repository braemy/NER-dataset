import lxml.etree as ET
import sys

from bz2 import BZ2File

from wikipedia_others.wiki_page import Wiki_page

"""
  <page>
    <title>AccessibleComputing</title>
    <ns>0</ns>
    <id>10</id>
    <redirect title="Computer accessibility" />
    <revision>
      <id>767284433</id>
      <parentid>631144794</parentid>
      <timestamp>2017-02-25T00:30:28Z</timestamp>
      <contributor>
        <username>Godsy</username>
        <id>23257138</id>
      </contributor>
      <comment>[[Template:This is a redirect]] has been deprecated, change to [[Template:Redirect category shell]].</comment>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text xml:space="preserve">#REDIRECT [[Computer accessibility]]

{{Redirect category shell|
{{R from move}}
{{R from CamelCase}}
{{R unprintworthy}}
}}</text>
      <sha1>ds1crfrjsn7xv73djcs4e4aq9niwanx</sha1>
    </revision>
  </page>


"""


class Wikipedia_iterator(object):
    def __init__(self):
        path = "/dlabdata1/braemy/data/en/small_enwiki.xml"

        self.iterator = ET.iterparse(path, encoding="utf-8", events=("start", "end"), recover=True)
        self.media_wiki_url ="{http://www.mediawiki.org/xml/export-0.10/}"
        self.current_event = ""
        self.current_tag = ""
        self.current_content = ""
        self.START = "start"
        self.END = "end"
        self.counter = 0

    def next_elem(self):
        self.current_event, tmp = next(self.iterator)
        self.current_tag = tmp.tag
        self.current_content = tmp.text
        tmp.clear()

        return self.current_tag



    def next_page(self):
        while not (self._is_page() and self._is_start_event()):
            self.next_elem()
            continue
        title = self._get_title()
        if title == 'Alabama':
            a = 1
        id = self._get_id()
        text = self._get_text()
        self._increment_counter()
        return Wiki_page(title, id, text)



    def _get_title(self):
        while not (self._is_title() and self._is_start_event()):
            self.next_elem()
            continue
        return self.current_content

    def _get_id(self):
        while not (self._is_id() and self._is_start_event()):
            self.next_elem()
            continue
        return self.current_content

    def _get_text(self):
        text = ""
        while (not self._is_text()):
            self.next_elem()
            continue
        while self._is_text():
            if self.current_content is not None:
                text += self.current_content
            self.next_elem()
        return text

    def _is_page(self):
        return self.current_tag == self.media_wiki_url + "page"

    def _is_title(self):
        return self.current_tag == self.media_wiki_url + "title"

    def _is_id(self):
        return self.current_tag == self.media_wiki_url + "id"
    def _is_text(self):
        return self.current_tag == self.media_wiki_url + "text"

    def _is_start_event(self):
        return self.current_event == self.START

    def _increment_counter(self):
        self.counter += 1
        if self.counter % 100000 == 0:
            print("{} pages processed".format(self.counter))