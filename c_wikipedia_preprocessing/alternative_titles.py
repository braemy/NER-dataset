# -*- coding: utf-8 -*-

import json
import regex
import sys
sys.path.append("../..")
from utils import *
from collections import Counter
from glob import glob
from tqdm import tqdm

def collect_all_outgoing_link(raw_text):
    outgoing_map = dict()
    reg = r"\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
    result = regex.finditer(reg, raw_text)
    for r in result:
        if ("#" and "{{" and "}}" and ":") not in r.group():
            origin_of_the_link = r.group(1) if r.group(3) is None else r.group(3)
            destination_of_the_link = r.group(1)
            origin_of_the_link = remove_quotes(origin_of_the_link)
            if destination_of_the_link not in outgoing_map:
                outgoing_map[destination_of_the_link] = [origin_of_the_link.strip()]
            else:
                outgoing_map[destination_of_the_link].append(origin_of_the_link.strip())
    return outgoing_map

def collect_title_disambiguation(parameters, title, text):
    #if the page is not a Entity => continue
    #[[True title (| how it appears in the text)]]
    alternative_title = title.split("({0})".format(parameters["disambiguation"]))[0].strip()
    #get the first bold link:
    title_to_alternatives = dict()
    reg = r"'''\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]'''"
    result = regex.search(reg, text)
    if result:
        title_to_alternatives[result.group(1)] = [alternative_title.strip("' ")]
    reg = r"\*+ *\[\[([\S\s]*?)(\|([\S\s]*?))?\]\]"
    result = regex.finditer(reg, text)
    for r in result:
        if r.group(1):
            title_to_alternatives[r.group(1)] = [alternative_title.strip("' ")]
    return title_to_alternatives

def collect_title_first_paragraph(title, text):
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


def collect_title_redirect(title, text):
    #if title not in wp_to_ner_by_title:
    #    return set()
    alternative_titles = set()
    alternative_titles.add(title)
    #if the title contains parenthesis, add also as altenative, the title without the parenthesis:
    # Spiderman (movie) -> Spiderman
    alternative_titles.add(remove_parenthesis(title))
    # for each each pages get the redirect as alternative
    redirect_reg1 = r"\{\{Redirect(\d*)\|([^\{\}]*)\}\}"
    alternative_titles = get_redirect(redirect_reg1, text, alternative_titles)
    redirect_reg2 = r"\{\{redirect-multi\|(\d*)\|([^\{\}]*)\}\}"
    alternative_titles = get_redirect(redirect_reg2, text, alternative_titles)
    alternative_titles = get_redirect(redirect_reg2, text, alternative_titles)
    return alternative_titles

def get_redirect(redirect_reg, text, alternative_titles):
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
    wp_to_ner_by_title = load_pickle(
        "{0}/{1}/wp_to_ner_by_title.p".format(parameters['wikidataNER_folder'], parameters['language']))
    redirect_titles = load_redirect(parameters)
    personal_titles = load_personal_titles(parameters)
    nationalities = load_nationalities(parameters)
    total = 0
    title_to_alternatives = dict()
    title_to_anchors = dict()
    anchor_to_titles = dict()
    title_to_outgoingTitle = dict()
    for file in tqdm(glob(input_file), "build_alternative_mapping"):
        with open(file, "r") as file:
            for line in file:
                total += 1
                data = json.loads(line)
                title = data['title']
                if not title:
                    continue
                links = data['links']
                alt = data['alt']
                disamb = data['disamb']
                title_to_outgoingTitle[title] = links.keys()
                if disamb:
                    for k, v in alt.items():
                        if k in personal_titles:
                            tmp = redirect_titles.get(k, set())
                            tmp.update(v)
                            personal_titles.update(tmp)
                        if k in nationalities:
                            tmp = redirect_titles.get(k, set())
                            tmp.update(v)
                            nationalities.update(tmp)

                        if k in personal_titles or k in nationalities or (k in wp_to_ner_by_title and wp_to_ner_by_title[k]["ner"] != "DISAM"):
                            tmp = redirect_titles.get(k, set())
                            tmp.update(v)
                            title_to_alternatives.setdefault(k, set()).update(tmp)
                if links:
                    for k, anchors in links.items():
                        remove_quotes(k)
                        title_to_anchors.setdefault(k, []).extend(anchors)
                        for anchor in anchors:
                            anchor_to_titles.setdefault(anchor, []).append(k)

    for k, v in title_to_alternatives.items():
        title_to_alternatives[k] = list(v)
    for k, v in anchor_to_titles.items():
        anchor_to_titles[k] = list(v)

    output_file = "{0}/{1}/title_to_alternatives.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(title_to_alternatives, output_file)
    output_file = "{0}/{1}/anchor_to_titles.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(anchor_to_titles, output_file)
    output_file = "{0}/{1}/title_to_anchors.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(title_to_anchors, output_file)

    output_file = "{0}/{1}/title_to_outgoingLinks.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(title_to_outgoingTitle, output_file)
    output_file = "{0}/{1}/personal_titles.p".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    pickle_data(personal_titles, output_file)

    output_file = "{0}/{1}/nationalities.p".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    pickle_data(nationalities, output_file)

def extend_alternative_titles(parameters):
    file_path = "{0}/{1}/title_to_alternatives.json".format(parameters['wikidataNER_folder'], parameters['language'])
    alternative_title = load_json(file_path)
    file_path = "{0}/{1}/title_to_anchors.json".format(parameters['wikidataNER_folder'], parameters['language'])
    title_to_anchors = load_json(file_path)
    personal_titles = load_personal_titles(parameters)
    nationalities = load_nationalities(parameters)

    for t, others in tqdm(title_to_anchors.items(), "extend_alternative_titles"):
        others = filter(lambda r: r, others)
        counter = Counter(others)
        new_titles = [k for k,v in counter.most_common() if v >5]
        for new_title in new_titles:
            if new_title[0].isupper():
                alternative_title.setdefault(t, []).append(new_title)

    new_personal_titles = set()
    for p in personal_titles:
        if p in title_to_anchors:
            anchors = title_to_anchors[p]
            others = filter(lambda r: r, anchors)
            counter = Counter(others)
            new_titles = [k for k, v in counter.most_common() if v > 8]
            if len(p.split()) == 1:
                new_personal_titles.add(p)
            for new_title in new_titles:
                if new_title[0].isupper() and len(new_title.split()) == 1:
                    new_personal_titles.add(new_title)

    new_nationalities = set()
    for n in nationalities:
        new_nationalities.add(n)
        if n in title_to_anchors:
            anchors = title_to_anchors[n]
            others = filter(lambda r: r, anchors)
            counter = Counter(others)
            new_titles = [k for k, v in counter.most_common() if v > 8]
            for new_title in new_titles:
                new_nationalities.add(new_title)


    alternative_title = {k:list(set(v)) for k,v in alternative_title.items()}

    output_file = "{0}/{1}/title_to_alternatives_extended.json".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    dump_json(alternative_title, output_file)
    output_file = "{0}/{1}/personal_titles.p".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    pickle_data(new_personal_titles, output_file)

    output_file = "{0}/{1}/nationalities_extended.p".format(
        parameters['wikidataNER_folder'],
        parameters['language']
    )
    pickle_data(new_nationalities, output_file)

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


def allowed_capitalised(parameters):
    wp_to_ner_class = load_pickle("{0}/{1}/wp_to_ner_by_title.p".format(parameters['wikidataNER_folder'], parameters['language']))
    wpTitle_to_wpid = load_wpTitle_to_wpId(parameters)
    input_file = "{0}/{1}/anchor_to_titles.json".format(parameters['wikidataNER_folder'], parameters['language'])
    anchor_to_titles = load_json(input_file)

    allowed_with_cap = set()
    for anchor, titles in tqdm(anchor_to_titles.items()):
        if anchor and anchor[0].isupper() and len(anchor.split()) == 1:# and len(titles) >= 50:
            c = Counter(titles)
            #titles = [k for k, v in c.items() if v > parameters["threshold_allowed_cap"]]
            total_occurrence = sum(c.values())
            if len(c) > 1:
                titles = [k for k, v in c.items() if float(v) / total_occurrence > 0.05]
                if titles:
                    include = True
                else:
                    include = False
                for title in titles:
                    if title not in wpTitle_to_wpid or title in wp_to_ner_class:# or is_list_of(title, parameters):
                        include = False
                        break
                if include:
                    allowed_with_cap.add(anchor)
                    for t in titles:
                        if len(t.split()) == 1:
                            allowed_with_cap.add(t)

    output_file = "{0}/{1}/allowed_with_cap.p".format(parameters['wikidataNER_folder'], parameters['language'])
    pickle_data(allowed_with_cap, output_file)