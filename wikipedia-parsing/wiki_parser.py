# -*- coding: utf-8 -*-
"""
Created by e-bug on 17/03/17.
"""

import re


# ==================================================================================================================== #
#                                                                                                                      #
#                                                   GLOBAL VARIABLES                                                   #
#                                                                                                                      #
# ==================================================================================================================== #

# Regular expressions # TODO -- handle '}}' in the article titles (though it is very unlikely to be found)
between_brackets_regex = '\[\[([^\[]*)\]\]'

main_articles_regex = '{{main article\|(.*?)(?:\|label\s\d+=.*?|\|l\d+=.*?|selfref=.*?|)}}'
details_regex = '{{details\|(?:topic=.*?\||)(.*?)(?:\|selfref=.*?|category=.*?|)}}'
see_also_regex = '{{see also\|(.*?)(?:\|label\s\d+=.*?|\|l\d+=.*?|selfref=.*?|category=.*?|)}}'
further_regex = '{{further(?: information\||\|)(.*?)(?:\|label\s\d+=.*?|\|l\d+=.*?|selfref=.*?|category=.*?|)}}'
about_regex = '{{about\|(.*?(?:{{!}}.*?|))(?:\|section=.*?|)}}'
about_disting_regex = '{{about-distinguish\|.*?\|(.*?(?:{{!}}.*?|))}}'
for_regex = '{{for\|(.*?)}}'
other_uses_regex = '{{other uses\|(.*?)}}'
other_uses_of_regex = '{{other uses of\|(.*?)}}'
other_uses_self_regex = '({{other uses}})'
other_uses_of_self_regex = '({{other uses of}})'
redir_regex = '{{redirect\|(.*?)(?:\|selfref=.*?|category=.*?|)}}'
distinguish_regex = '{{distinguish\|(.*?)}}'
redir_disting_regex = '{{redirect-distinguish\|.*?\|(.*?)}}'

hatnotes_regex = '|'.join([main_articles_regex, details_regex, see_also_regex, further_regex, other_uses_regex,
                           distinguish_regex, redir_disting_regex,
                           about_regex, about_disting_regex, for_regex, other_uses_of_regex, other_uses_self_regex,
                           other_uses_of_self_regex, redir_regex])
all_regex = '|'.join([between_brackets_regex,
                      main_articles_regex, details_regex, see_also_regex, further_regex, other_uses_regex,
                      distinguish_regex, redir_disting_regex,
                      about_regex, about_disting_regex, for_regex, other_uses_of_regex, other_uses_self_regex,
                      other_uses_of_self_regex, redir_regex])


# ==================================================================================================================== #
#                                                                                                                      #
#                                                   EXTRACTING LINKS                                                   #
#                                                                                                                      #
# ==================================================================================================================== #

def get_hatnotes_links(lowercase_unicode_page, page_title):
    """
    Extract standard outgoing links from the of an article.
    Extract outgoing links from several hatnote templates in the article: main article, details, see also,
    further information, about, about-distinguish, for, other uses, other uses of, redirect,
    distinguish, redirect-distinguish.
    :param lowercase_unicode_page: lowered string containing the text of an article
    :param page_title: title of the article (if None, some links will be neglected)
    :return: list of links to other articles in the passed page
    """

    if lowercase_unicode_page is None:
        return []

    if page_title is not None:
        formatted_page_title = page_title.lower()

    # **************************************************************************************************************** #
    #                                           More pages on the same topic                                           #
    # **************************************************************************************************************** #

    # Retrieve all the possible "main article" links: "Main article: …"
    main_articles_links = re.findall(main_articles_regex, lowercase_unicode_page)

    # Retrieve all the possible "details" links: "For more details on …, see …"
    details_links = re.findall(details_regex, lowercase_unicode_page)

    # Retrieve all the possible "see also" article links: "See also …"
    see_also_links = re.findall(see_also_regex, lowercase_unicode_page)

    # Retrieve all the possible "further information" article links: "Further information: …"
    further_links = re.findall(further_regex, lowercase_unicode_page)

    # **************************************************************************************************************** #
    #                                           Other uses of the same title                                           #
    # **************************************************************************************************************** #

    # Retrieve all the possible "about" article links: "This page is about … For other uses …"
    about_contents = re.findall(about_regex, lowercase_unicode_page)
    about_links = []
    for content in about_contents:
        splitted_content = content.split('|')
        splitted_content_len = len(splitted_content)
        if ((splitted_content_len == 1) or (splitted_content_len == 3 and splitted_content[1] == '')) and \
            page_title is not None:
            about_links.append(formatted_page_title + ' (disambiguation)')
        else:
            # general case: USE1|USE2|P1|and|P2|other uses
            i = 2  # skip USE1
            while i < splitted_content_len:
                about_links.append(splitted_content[i].replace('{{!}}', '#')) # "#" is dividing char in deltas_utils.py
                i += 1
                if (i < splitted_content_len) and (splitted_content[i] == 'other uses') and \
                   (i + 1 >= splitted_content_len) and (page_title is not None):
                    about_links.append(formatted_page_title + ' (disambiguation)')
                i += 1

    # Retrieve all the possible "about-distinguish" article links:"This page is about … It is not to be confused with …"
    about_disting_links = re.findall(about_disting_regex, lowercase_unicode_page)
    for i,string in enumerate(about_disting_links):
        about_disting_links[i] = string.replace('{{!}}', '#') # we use "#" as dividing char in deltas_utils.py

    # Retrieve all the possible "for" article links: "For …, see …"
    for_contents = re.findall(for_regex, lowercase_unicode_page)
    for_links = []
    for content in for_contents:
        splitted_content = content.split('|')
        splitted_content_len = len(splitted_content)
        if (splitted_content_len == 1) and (page_title is not None):
            for_links.append(formatted_page_title + ' (disambiguation)')
        else:
            for i in range(1, splitted_content_len):
                for_links.append(splitted_content[i])

    # Retrieve all the possible "other uses" article links: "For other uses, see …"
    other_uses_links = re.findall(other_uses_regex, lowercase_unicode_page)

    # Retrieve all the possible "other uses of" article links: "For other uses of …, see …"
    other_uses_of_contents = re.findall(other_uses_of_regex, lowercase_unicode_page)
    other_uses_of_links = []
    for content in other_uses_of_contents:
        splitted_content = content.split('|')
        splitted_content_len = len(splitted_content)
        if (splitted_content_len == 1):
            other_uses_of_links.append(splitted_content[0].lower() + ' (disambiguation)')
        elif (splitted_content_len == 2):
            other_uses_of_links.append(splitted_content[1])

    # Retrieve "other uses" and "other uses of" self disambiguation links
    if page_title is not None:
        links = re.findall(other_uses_self_regex, lowercase_unicode_page)
        for _ in links:
            other_uses_links.append(formatted_page_title + ' (disambiguation)')
    if page_title is not None:
        links = re.findall(other_uses_of_self_regex, lowercase_unicode_page)
        for _ in links:
            other_uses_of_links.append(formatted_page_title + ' (disambiguation)')


    # Retrieve all the possible "redirect" article links: "… redirects here. For other uses, see …"
    redir_contents = re.findall(redir_regex, lowercase_unicode_page)
    redir_links = []
    for content in redir_contents:
        splitted_content = content.split('|')
        splitted_content_len = len(splitted_content)
        if splitted_content_len == 1:
            redir_links.append(splitted_content[0].lower() + ' (disambiguation)')
        else:
            # general case: P0|USE1|P1|USE2|P2|and|P3
            i = 2  # skip P0 and USE1
            while i < splitted_content_len:
                redir_links.append(splitted_content[i])
                i += 2

    # **************************************************************************************************************** #
    #                                                   Distinguish                                                    #
    # **************************************************************************************************************** #

    # Retrieve all the possible "distinguish" article links: "Not to be confused with …"
    distinguish_links = re.findall(distinguish_regex, lowercase_unicode_page)

    # Retrieve all the possible "redirect-distinguish" article links: "... Not to be confused with ..."
    redir_disting_links = re.findall(redir_disting_regex, lowercase_unicode_page)


    # **************************************************************************************************************** #

    # All the hatnote templates links
    templates_links = []
    for link in main_articles_links:
        templates_links.extend(link.split('|'))
    for link in details_links:
        templates_links.extend(link.split('|'))
    for link in see_also_links:
        templates_links.extend(link.split('|'))
    for link in further_links:
        templates_links.extend(link.split('|'))
    for link in about_links:
        templates_links.append(link)
    for link in about_disting_links:
        templates_links.extend(link.split('|'))
    for link in for_links:
        templates_links.append(link)
    for link in other_uses_links:
        templates_links.extend(link.split('|'))
    for link in other_uses_of_links:
        templates_links.append(link)
    for link in redir_links:
        templates_links.append(link)
    for link in distinguish_links:
        templates_links.extend(link.split('|'))
    for link in redir_disting_links:
        templates_links.extend(link.split('|'))

    return templates_links


def get_hatnotes_links_fast(lowercase_unicode_page, page_title):
    """
    Extract outgoing links from several hatnote templates in the article: main article, details, see also,
    further information, about, about-distinguish, for, other uses, other uses of, redirect,
    distinguish, redirect-distinguish.
    :param lowercase_unicode_page: lowered string containing the text of an article
    :param page_title: title of the article
    :return: list of links to other articles in the passed page
    """
    # FIXME -- Piping regexs together into one does not extract any links for some articles

    if lowercase_unicode_page is None:
        return []

    formatted_page_title = page_title.lower()

    # **************************************************************************************************************** #

    # Retrieve all the possible hatnote templates links
    hatnotes_tuples = re.findall(hatnotes_regex, lowercase_unicode_page)

    # Split in lists that require the same processing
    hatnotes_links = []
    for tuple_ in hatnotes_tuples:
        idx, content = next((i, s) for (i, s) in enumerate(tuple_) if s)
        if idx < 7: # no processing required
            hatnotes_links.extend(content.split('|'))
        elif idx == 7: # process "about" article links: "This page is about … For other uses …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if (splitted_content_len == 1) or (splitted_content_len == 3 and splitted_content[1] == ''):
                hatnotes_links.append(formatted_page_title + ' (disambiguation)')
            else:
                # general case: USE1|USE2|P1|and|P2|other uses
                i = 2  # skip USE1
                while i < splitted_content_len:
                    hatnotes_links.append(splitted_content[i].replace('{{!}}', '#'))
                    i += 1
                    if (i < splitted_content_len) and (splitted_content[i] == 'other uses') and \
                       (i + 1 >= splitted_content_len):
                        hatnotes_links.append(formatted_page_title + ' (disambiguation)')
                    i += 1
        elif idx == 8: # process "about-distinguish" article links:"This page is about… It is not to be confused with…"
            hatnotes_links.append(content.replace('{{!}}', '#')) # we use "#" as dividing char in deltas_utils.py
        elif idx == 9: # process "for" article links: "For …, see …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if splitted_content_len == 1:
                hatnotes_links.append(formatted_page_title + ' (disambiguation)')
            else:
                for i in range(1, splitted_content_len):
                    hatnotes_links.append(splitted_content[i])
        elif idx == 10: # process "other uses of" article links: "For other uses of …, see …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if (splitted_content_len == 1):
                hatnotes_links.append(splitted_content[0].lower() + ' (disambiguation)')
            elif (splitted_content_len == 2):
                hatnotes_links.append(splitted_content[1])
        elif idx == 11 or idx == 12: # process "other uses" and "other uses of" self disambiguation links
            hatnotes_links.append(formatted_page_title + ' (disambiguation)')
        else: # process "redirect" article links: "… redirects here. For other uses, see …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if splitted_content_len == 1:
                hatnotes_links.append(splitted_content[0].lower() + ' (disambiguation)')
            else:
                # general case: P0|USE1|P1|USE2|P2|and|P3
                i = 2  # skip P0 and USE1
                while i < splitted_content_len:
                    hatnotes_links.append(splitted_content[i])
                    i += 2

    # **************************************************************************************************************** #

    return hatnotes_links


def get_outlinks(lowercase_unicode_page):
    """
    Extract standard outgoing links from the of an article (text between double brackets).
    :param lowercase_unicode_page: lowered string containing the text of an article
    :return: list of links to other articles in the passed page
    """

    if lowercase_unicode_page is None:
        return []

    # Retrieve all the possible links between brackets
    between_brackets_links = re.findall(between_brackets_regex, lowercase_unicode_page)

    return between_brackets_links


def get_standard_and_hatnotes_links_fast(lowercase_unicode_page, page_title):
    """
    Extract standard outgoing links and outgoing links from several hatnote templates in the article:
    main article, details, see also, further information, about, about-distinguish, for, other uses, other uses of,
    redirect, distinguish, redirect-distinguish.
    :param lowercase_unicode_page: lowered string containing the text of an article
    :param page_title: title of the article
    :return: list of standard outgoing links, list of hatnotes links
    """
    # FIXME -- Piping regexs together into one does not extract any links for some articles

    if lowercase_unicode_page is None:
        return [], []

    formatted_page_title = page_title.lower()

    # **************************************************************************************************************** #

    # Retrieve all possible links
    all_links_tuples = re.findall(all_regex, lowercase_unicode_page)

    # Split in lists that require the same processing
    standard_links = []
    hatnotes_links = []
    for tuple_ in all_links_tuples:
        idx, content = next((i, s) for (i, s) in enumerate(tuple_) if s)
        if idx == 0:
            standard_links.append(content)
        elif idx < 8: # no processing required
            hatnotes_links.extend(content.split('|'))
        elif idx == 8: # process "about" article links: "This page is about … For other uses …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if (splitted_content_len == 1) or (splitted_content_len == 3 and splitted_content[1] == ''):
                hatnotes_links.append(formatted_page_title + ' (disambiguation)')
            else:
                # general case: USE1|USE2|P1|and|P2|other uses
                i = 2  # skip USE1
                while i < splitted_content_len:
                    hatnotes_links.append(splitted_content[i].replace('{{!}}', '#'))
                    i += 1
                    if (i < splitted_content_len) and (splitted_content[i] == 'other uses') and \
                       (i + 1 >= splitted_content_len):
                        hatnotes_links.append(formatted_page_title + ' (disambiguation)')
                    i += 1
        elif idx == 9: # process "about-distinguish" article links:"This page is about… It is not to be confused with…"
            hatnotes_links.append(content.replace('{{!}}', '#')) # we use "#" as dividing char in deltas_utils.py
        elif idx == 10: # process "for" article links: "For …, see …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if splitted_content_len == 1:
                hatnotes_links.append(formatted_page_title + ' (disambiguation)')
            else:
                for i in range(1, splitted_content_len):
                    hatnotes_links.append(splitted_content[i])
        elif idx == 11: # process "other uses of" article links: "For other uses of …, see …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if (splitted_content_len == 1):
                hatnotes_links.append(splitted_content[0].lower() + ' (disambiguation)')
            elif (splitted_content_len == 2):
                hatnotes_links.append(splitted_content[1])
        elif idx == 12 or idx == 13: # process "other uses" and "other uses of" self disambiguation links
            hatnotes_links.append(formatted_page_title + ' (disambiguation)')
        else: # process "redirect" article links: "… redirects here. For other uses, see …"
            splitted_content = content.split('|')
            splitted_content_len = len(splitted_content)
            if splitted_content_len == 1:
                hatnotes_links.append(splitted_content[0].lower() + ' (disambiguation)')
            else:
                # general case: P0|USE1|P1|USE2|P2|and|P3
                i = 2  # skip P0 and USE1
                while i < splitted_content_len:
                    hatnotes_links.append(splitted_content[i])
                    i += 2

    # **************************************************************************************************************** #

    return standard_links, hatnotes_links


# ==================================================================================================================== #
#                                                                                                                      #
#                                        TRANSFORMING TEXT AND EXTRACTING LINKS                                        #
#                                                                                                                      #
# ==================================================================================================================== #

def normalize_title(title):
    """
    Standardize a title of an article by replacing white spaces with underscores and capitalizing the first letter only.  
    :param title: the title of an article (string)
    :return: normalized title (string)
    """
    return title.replace(' ', '_').capitalize()


def get_hatnotes_outlinks_in_page(unicode_page, page_title):
    """
    Retrieve the titles of the articles pointed by the page in its hatnote templates.
    :param unicode_page: unicode string containing the text of a Wikipedia article
    :param page_title: title of the article
    :return: list of all article titles: ['Art 1', ..] (with duplicates)
    """

    if unicode_page is None:
        return []

    # Replace '\n' with whitespace
    spaced_text = unicode_page.replace('\n', ' ')

    # Set text in lowercase
    lower_text = spaced_text.lower()

    # Remove <imagemap>'s
    filtered_text = re.sub('<imagemap>.+</imagemap>', '', lower_text)

    # All the hatnote template links
    templates_links = get_hatnotes_links_fast(filtered_text, page_title)

    return templates_links


def get_all_outlinks_in_page(unicode_page, page_title):
    """
    Retrieve the titles of all the articles pointed by the page in its text.
    :param unicode_page: unicode string containing the text of a Wikipedia article
    :param page_title: title of the article
    :return: list of all article titles: ['Art 1', ..] (with duplicates)
    """

    if unicode_page is None:
        return []

    # Replace '\n' with whitespace
    spaced_text = unicode_page.replace('\n', ' ')

    # Set text in lowercase
    lower_text = spaced_text.lower()

    # Remove <imagemap>'s
    filtered_text = re.sub('<imagemap>.+</imagemap>', '', lower_text)

    # Retrieve all the possible links
    between_brackets_links, templates_links = get_standard_and_hatnotes_links_fast(filtered_text, page_title)

    # All the links combined
    all_links = []
    all_links.extend(between_brackets_links)
    all_links.extend(templates_links)

    title_outlinks = []
    for link in all_links:
        # Define the links we should get rid of
        image = link.startswith('image:')
        file_ = link.startswith('file:')
        self_section = link.startswith('#')

        # If the link does not point to
        # - an image
        # - a file
        # - a section in the same article
        if (image is False) and (file_ is False) and (self_section is False):
            # Get the hyperlink's article title
            title = link.split('|')[0].split('#')[0].capitalize()

            # Append the title to the list
            title_outlinks.append(title)

    return title_outlinks


def get_all_and_hatnotes_outlinks_in_page(unicode_page, page_title):
    """
    Retrieve the titles of all the articles and of only the articles in hatnotes pointed by the page in its text.
    :param unicode_page: unicode string containing the text of a Wikipedia article
    :param page_title: title of the article
    :return: list of all article titles: ['Art 1', ..] (with duplicates),
             list of article in hatnotes titles: ['Art 1', ..] (with duplicates)
    """

    if unicode_page is None:
        return [], []

    # Replace '\n' with whitespace
    spaced_text = unicode_page.replace('\n', ' ')

    # Set text in lowercase
    lower_text = spaced_text.lower()

    # Remove <imagemap>'s
    filtered_text = re.sub('<imagemap>.+</imagemap>', '', lower_text)

    # Retrieve all the possible links
    between_brackets_links = get_outlinks(filtered_text)
    templates_links = get_hatnotes_links(filtered_text, page_title)

    # All the links combined
    all_links = []
    all_links.extend(between_brackets_links)
    all_links.extend(templates_links)

    all_outlinks = []
    for link in all_links:
        # Define the links we should get rid of
        image = link.startswith('image:')
        file_ = link.startswith('file:')
        self_section = link.startswith('#')

        # If the link does not point to
        # - an image
        # - a file
        # - a section in the same article
        if (image is False) and (file_ is False) and (self_section is False):
            # Get the hyperlink's article title
            title = link.split('|')[0].split('#')[0].capitalize()

            # Append the title to the list
            all_outlinks.append(title)

    # Hatnotes links only
    hatnotes_outlinks = []
    for link in templates_links:
        # Define the links we should get rid of
        image = link.startswith('image:')
        file_ = link.startswith('file:')
        self_section = link.startswith('#')

        # If the link does not point to
        # - an image
        # - a file
        # - a section in the same article
        if (image is False) and (file_ is False) and (self_section is False):
            # Get the hyperlink's article title
            title = link.split('|')[0].split('#')[0].capitalize()

            # Append the title to the list
            hatnotes_outlinks.append(title)

    return all_outlinks, hatnotes_outlinks


# ==================================================================================================================== #
#                                                                                                                      #
#                                     TRANSFORMING TEXT AND EXTRACTING UNIQUE LINKS                                    #
#                                                                                                                      #
# ==================================================================================================================== #

def get_unique_hatnotes_outlinks_in_page(unicode_page, page_title):
    """
    Retrieve the titles of the articles pointed by the page in its hatnote templates.
    :param unicode_page: unicode string containing the text source of a Wikipedia article
    :param page_title: title of the article
    :return: list of all article titles: ['Art 1', ..] (no duplicates)
    """

    # Retrieve hatnotes outlinks (with duplicates)
    outlinks_list = get_hatnotes_outlinks_in_page(unicode_page, page_title)

    unique_outlinks_list = list(set(outlinks_list))

    return unique_outlinks_list


def get_unique_all_outlinks_in_page(unicode_page, page_title):
    """
    Retrieve the titles of all the articles pointed by the page in its text.
    :param unicode_page: unicode string containing the text source of a Wikipedia article
    :param page_title: title of the article
    :return: list of all article titles: ['Art 1', ..] (no duplicates)
    """

    # Retrieve all outlinks (with duplicates)
    outlinks_list = get_all_outlinks_in_page(unicode_page, page_title)

    unique_outlinks_list = list(set(outlinks_list))

    return unique_outlinks_list


# ==================================================================================================================== #
#                                                                                                                      #
#                               TRANSFORMING TEXT AND EXTRACTING LINKS WITH MULTIPLICITY                               #
#                                                                                                                      #
# ==================================================================================================================== #

def count_multiplicity_in_list(list_):
    """
    Compute the frequency of each item in the passed list.
    :param list_: list of items
    :return: list of multiplicities in the same order as the items when doing set(list_) or list(set(list_))
    """

    frequency_list = []
    for item in set(list_):
        f = list_.count(item)
        frequency_list.append(str(f))

    return frequency_list


def get_page_hatnotes_outlinks_with_frequency(unicode_page, page_title):
    """
    Retrieve the titles of the articles pointed by the page in the hatnotes in its text and their multiplicity.
    :param unicode_page: unicode string containing the text source of a Wikipedia page
    :param page_title: title of the article
    :return: list of article titles with their frequency in templates: [('Art 1', f1), ..]
    """

    # Retrieve hatnotes outlinks (with duplicates)
    outlinks_list = get_hatnotes_links_fast(unicode_page, page_title)

    # Count their multiplicity
    outlinks_frequency_list = count_multiplicity_in_list(outlinks_list)

    title_freq_list = list(zip(set(outlinks_list), outlinks_frequency_list))

    return title_freq_list


def get_page_all_outlinks_with_frequency(unicode_page, page_title):
    """
    Retrieve the titles of all the articles pointed by the page in its text and their multiplicity.
    :param unicode_page: unicode string containing the text source of a Wikipedia page
    :param page_title: title of the article
    :return: list of all article titles with their frequency: [('Art 1', f1), ..]
    """

    # Retrieve all outlinks (with duplicates)
    outlinks_list = get_all_outlinks_in_page(unicode_page, page_title)

    # Count their multiplicity
    outlinks_frequency_list = count_multiplicity_in_list(outlinks_list)

    title_freq_list = list(zip(set(outlinks_list), outlinks_frequency_list))

    return title_freq_list


def get_page_all_and_hatnotes_outlinks_with_frequency(unicode_page, page_title):
    """
    Retrieve the titles of all the articles and of only the articles in hatnotes pointed by the page in its text and
    their multiplicity.
    :param unicode_page: unicode string containing the text source of a Wikipedia page
    :param page_title: title of the article
    :return: list of two lists:
             - list of all article titles with their frequency: [('Art 1', f1), ..]
             - list of hatnotes article titles with their frequency: [('Art 1', f1), ..]
    """

    # Retrieve all outlinks (with duplicates)
    all_outlinks_list, hatnotes_outlinks_list = get_all_and_hatnotes_outlinks_in_page(unicode_page, page_title)

    # Count their multiplicity
    all_outlinks_frequency_list = count_multiplicity_in_list(all_outlinks_list)
    hatnotes_outlinks_frequency_list = count_multiplicity_in_list(hatnotes_outlinks_list)

    all_title_freq_list = list(zip(set(all_outlinks_list), all_outlinks_frequency_list))
    hatnotes_title_freq_list = list(zip(set(hatnotes_outlinks_list), hatnotes_outlinks_frequency_list))

    return list((all_title_freq_list, hatnotes_title_freq_list))
