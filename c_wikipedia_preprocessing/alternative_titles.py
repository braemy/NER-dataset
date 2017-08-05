# -*- coding: utf-8 -*-

import json
import regex
import sys
sys.path.append("../..")
from utils import *
from collections import Counter
from glob import glob
from tqdm import tqdm

def collect_all_outgoing_link(raw_text, wp_to_ner_by_title):
    outgoing_map = dict()
    reg = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
    result = regex.finditer(reg, raw_text)
    for r in result:
        if ("#" and "{{" and "}}") not in r.group():
            origin_of_the_link = r.group(1) if r.group(3) is None else r.group(3)
            destination_of_the_link = r.group(1)
            #if r.group(1) in wp_to_ner_by_title:
            origin_of_the_link = remove_quotes(origin_of_the_link)
            if destination_of_the_link not in outgoing_map:
                outgoing_map[destination_of_the_link] = [origin_of_the_link]
            else:
                outgoing_map[destination_of_the_link].append(origin_of_the_link)
    return outgoing_map

def collect_title_disambiguation(title, text, wp_to_ner_by_title):
    #if the page is not a Entity => continue
    #[[True title (| how it appears in the text)]]
    alternative_title = title.split("(disambiguation)")[0].strip()
    #get the first bold link:
    title_to_alternatives = dict()
    reg = r"'''\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]'''"
    result = regex.search(reg, text)
    if result:# and result.group(1) in wp_to_ner_by_title:
        title_to_alternatives[result.group(1)] = [alternative_title.strip("' ")]
    reg = r"\*+ *\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
    result = regex.finditer(reg, text)
    for r in result:
        if r.group(1):# in wp_to_ner_by_title:
            title_to_alternatives[r.group(1)] = [alternative_title.strip("' ")]
    return title_to_alternatives

def collect_title_first_paragraph(title, text, wp_to_ner_by_title):
    # the first sentence contains many alternative title in bold. Parse the first sentence to get such title

    # for each page include the title
    #if title not in wp_to_ner_by_title:
    #    return set()
    alternative_titles = set()
    alternative_titles.add(title)
    first_paragraph = text.split("\n\n")[0]
    reg = r"'''([\S\s]*?)'''"
    result = regex.finditer(reg, first_paragraph)
    for r in result:
        t = r.group(1).strip("' ")
        t = remove_quotes(t)
        alternative_titles.add(t)
    return list(alternative_titles)


def collect_title_redirect(title, text, wp_to_ner_by_title):
    #if title not in wp_to_ner_by_title:
    #    return set()
    alternative_titles = set()
    alternative_titles.add(title)
    #if the title contains parenthesis, add also as altenative, the title without the parenthesis:
    # Spiderman (movie) -> Spiderman
    alternative_titles.add(remove_parenthesis(title))
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
            redirect = remove_quotes(redirect)
            alternative_titles.add(redirect.strip("' "))
    return alternative_titles


def remove_quotes(k):
    return regex.sub("''", "", k)

def remove_parenthesis(k):
    return regex.sub(r"\(.*?\)", "", k).strip()


def build_altenative_mapping(parameters):

    input_file = "{0}/{1}/link_infos/part*".format(parameters['wikidataNER_folder'], parameters['language'])
    wp_to_ner_by_title = load_pickle("{0}/{1}/wp_to_ner_by_title.p".format(parameters['wikidataNER_folder'], parameters['language']))
    total = 0
    name_from_link = dict()
    alternative_title = dict()
    outgoingLink_by_title = dict()
    for file in tqdm(glob(input_file), "build_alternative_mapping" ):
        with open(file, "r") as file:
            for line in file:
                total += 1
                data = json.loads(line)
                title = data['title']
                links = data['links']
                alt = data['alt']
                outgoingLink_by_title[title] = links.keys()

                if links:
                    for k, v in links.items():
                        remove_quotes(k)
                        if k in name_from_link:
                            name_from_link[k].extend(v)
                        else:
                            name_from_link[k] = v

                if title not in wp_to_ner_by_title: #we care only about Entities
                    continue
                if alt:
                    for k, v in alt.items():
                        if k in alternative_title:
                            alternative_title[k].union(v)
                        else:
                            alternative_title[k] = set(v)


    for k, v in alternative_title.items():
        alternative_title[k] = list(v)
    for k, v in name_from_link.items():
        name_from_link[k] = list(v)

    output_file = "{0}/{1}/alternative_titles.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(alternative_title, output_file)
    output_file = "{0}/{1}/titles_from_links.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(name_from_link, output_file)
    output_file = "{0}/{1}/outgoingLinks_by_titles.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(outgoingLink_by_title, output_file)

def extend_alternative_titles(parameters):
    file_path = "{0}/{1}/alternative_titles.json".format(parameters['wikidataNER_folder'], parameters['language'])
    alternative_title = load_json(file_path)
    file_path = "{0}/{1}/titles_from_links.json".format(parameters['wikidataNER_folder'], parameters['language'])
    name_from_link = load_json(file_path)

    for t, others in tqdm(name_from_link.items(), "extend_alternative_titles"):
        others = filter(lambda r: r, others)
        counter = Counter(others)
        if t in alternative_title:
            new_titles = counter = [k for k,v in counter.most_common() if v >3]
            for new_title in new_titles:
                if new_title[0].isupper():
                    if new_title not in alternative_title[t]:
                        alternative_title[t].append(new_title)

            #alternative_title[t].extend(list(map(lambda x: x[0], filter(lambda x: x[1] > 50, counter.items()))))
        else:
            if new_title[0].isupper():
                alternative_title[t] = [new_title]


    output_file = "{0}/{1}/alternative_titles_extended.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(alternative_title, output_file)


def remove_non_entities_using_link(parameters):
    wp_to_ner_class = load_pickle("{0}/{1}/wp_to_ner_by_title.p".format(parameters['wikidataNER_folder'], parameters['language']))
    title_from_links = load_json("{0}/{1}/titles_from_links.json".format(parameters['wikidataNER_folder'], parameters['language']))

    wp_to_ner_class_cleaned = dict()
    for i, (wp_title, value) in tqdm(enumerate(wp_to_ner_class.items()), "remove_non_entities_using_link"):
        lowercase = bool(value["lc"])
        ner_class = value["ner"]
        if wp_title not in title_from_links:
            continue
        links = title_from_links[wp_title]
        counter = Counter(links)
        substring_of_the_target = []
        for t, c in counter.items():
            if t.lower() in wp_title.lower():
                substring_of_the_target.extend([t] * c)

        ratio_lower = sum(map(lambda x: x.islower(), substring_of_the_target)) / len(
            substring_of_the_target) if len(substring_of_the_target) else 0
        if not lowercase and len(substring_of_the_target) >= 5 and ratio_lower > 0.3 and wp_title.split()[0] not in ["A",
                                                                                                          "An",
                                                                                                          "The"]:
            print(wp_title, ratio_lower)


def anchor_to_articles(parameters):
    title_from_links = load_json("{0}/{1}/titles_from_links.json".format(parameters['wikidataNER_folder'], parameters['language']))
    anchor_to_articles = dict()
    for title, anchors in tqdm(title_from_links.items()):
        for anchor in set(anchors):
            if anchor not in anchor_to_articles:
                anchor_to_articles[anchor] = [title]
            elif title not in anchor_to_articles[anchor]:
                anchor_to_articles[anchor].append(title)

    output_file =  "{0}/{1}/anchor_to_articles.json".format(parameters['wikidataNER_folder'], parameters['language'])
    dump_json(anchor_to_articles, output_file)


def allowed_capitalised(parameters):
    wp_to_ner_class = load_pickle("{0}/{1}/wp_to_ner_by_title.p".format(parameters['wikidataNER_folder'], parameters['language']))
    input_file = "{0}/{1}/anchor_to_articles.json".format(parameters['wikidataNER_folder'], parameters['language'])
    anchor_to_articles = load_json(input_file)

    allowed_with_cap = []
    for anchor, titles in tqdm(anchor_to_articles.items()):
        for title in titles:
            if is_disambiguation(title) or is_list_of(title, parameters):
                break
            elif title in wp_to_ner_class:
                break
            else:
                allowed_with_cap.append(anchor)

    output_file = "{0}/{1}/allowed_with_cap.json".format(parameters['wikidataNER_folder'], parameters['language'])
    dump_json(allowed_with_cap, output_file)