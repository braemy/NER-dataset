import regex

def remove_html_balise(text):
    list = r"<\/?(ol|li|onlyinclude).*?>"  # <ol start="4">
    text = regex.sub(list, "", text)
    return text

def remove_nowiki(text):
    # <nowiki>[that it]</nowiki>
    nowiki = r"<nowiki>.*?<\/nowiki>"
    text = regex.sub(nowiki, "", text)
    return text

def remove_center(text):
    center = r"<center>[\S\s]*?<\/center>"
    text = regex.sub(center, "", text)
    return text

def remove_quote(text):
    quote = r"\'\'\'"
    split = text.strip().split("\n")
    second_part = regex.sub(quote, "", "\n".join(split[1:]))  # because in the first paragraph we need to keep
    # them to distinguish the title
    text = split[0] + "\n\n" + second_part
    return text

def remove_algorithm(text):
    """
    {{algorithm-begin|name=LargestNumber}}
    ...
    {{algorithm-end}}
    """
    alg = r"\{\{algorithm-begin.*\}\}[\S\s]*?\{\{algorithm-end\}\}"
    text = regex.sub(alg, "", text)
    return text
def remove_others(text):

    reg = r" *longd\d+\|.*" #longd54|longsW
    text =  regex.sub(reg, "", text)
    reg = r" *leftarm.*\|.*" #leftarm1990033|rightarm182CAFF|socks1=990033
    text = regex.sub(reg, "", text)
    reg  = "^*File:.*\|imagesize="
    text = regex.sub(reg, "", text)
    reg = "coordinates_display=%" #coordinates_display=%
    text = regex.sub(reg, "", text)
    reg = "^ *elevation *=.*%" #elevation = 590%
    text = regex.sub(reg, "", text)
    reg = "^ *coordinates *=.*%" #coordinates = %
    text = regex.sub(reg, "", text)
    reg = "^ *longd=\d+(.\d+)=%" #longd=16.44269
    text = regex.sub(reg, "", text)
    reg = "<span.*?>.*?<\/span>" #<span style="white-space:nowrap"> {{convert|-33|C|F|1|disp=or}} </span>
    text = regex.sub(reg, "", text)
    return text

def remove_parenthesis_first_sentence(text):
    #usually contains prononciation and of title that we can't infer
    parenthesis = r"\(.*?\)"
    sentences = text.split(".")
    first = regex.sub(parenthesis, "", sentences[0])
    text = first +"." + ".".join(sentences[1:])
    return text
def remove_gallery(text):
    gallery = r"<gallery[^\/]*?>[\S\s]*?<\/gallery>"
    return regex.sub(gallery, "", text)

def remove_italic(text):
    def replace(o):
        return o.group(1) + o.group(3)
    italic = r"([^']|^)('')([^'])"
    return regex.sub(italic, replace, text, flags=regex.MULTILINE)

def remove_source(text):
    div_regex = r"<source[^\/]*?>[\S\s]*?<\/source>"
    text = regex.sub(div_regex, "", text)
    source = r"^('')?Source:.*"
    return regex.sub(source, "", text, flags=regex.MULTILINE)

def remove_br(text):
    br = r"< *(br|BR) *\/? *>"
    text = regex.sub(br, "\n", text)
    return text

def remove_ref(text):
    #ref_regex = r"<ref[^\/]*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
    ref_regex = r"<ref.*?>[\S\s]*?<\/ref>|<ref[\S\s]*?\/>"
    #ref_regex = r"<ref[^\/]*?>([^<]|(<ref[^\/]*?>([^<])*?<\/ref>|<ref[\S\s]*?\/>))*?<\/ref>|<ref[\S\s]*?\/>"
    #ref_regex =  r"<ref[^\/]*?>([^<]|(?R))*?<\/ref>|<ref[\S\s]*?\/>"
    text = regex.sub(ref_regex, "", text)
    #remove everything that is below the section == References ==
    if text.find("== (References|Quellen) ==") >= 0:
        text = text[:text.find("== (References|Quellen) ==")]
    if text.find("==(References|Quellen)==") >= 0:
        text = text[:text.find("==(References|Quellen)==")]
    return text



def remove_block(text, open_, close):
    start = [(m.start(), m.end(), open_) for m in regex.finditer(open_, text)]
    end = [(m.start(), m.end(), close) for m in regex.finditer(close, text)]
    start.extend(end)
    braces = sorted(start, key=lambda x: x[0])
    opened = 0
    start_list = []
    end_list = []
    scale = 0
    for s, e, p in braces:
        if p == open_:
            opened += 1
            start_list.append(s)
        if p == close:
            opened -= 1
            end_list.append(e)
        if opened == 0 and start_list and end_list:
            s = start_list.pop(0)
            e = end_list.pop()
            text = text.replace(text[s - scale:e - scale], "", 1)
            scale += e - s
            start_list = []
            end_list = []
    return text




def remove_table(text):
    #reg = r"^( |-)*\{\{.*"  # {{Authority control}}
    #text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    #reg = r"^ *\}\}" #}}
    #text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    #reg = r"^ *\|.*" #|list # | fkodkf
    #text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r"^ *\{\|.*" #{| class
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r"^ *\|\}"  # |}
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    #sometimes the text is at the beginning:
    reg = r".*\}\} *$" #}}
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    #reg = r"^.*\| *$" #fkodkf|
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r".*\| *\} *$"  # |}
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r"^----"  # |}
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    return text

def remove_comment(text):
    comment_regex = r"<!--[\S\s]*?-->"
    return regex.sub(comment_regex, "", text)

def remove_list(text):
    list_regex = r"^(#|:|;|!)+.*?"
    return regex.sub(list_regex, "", text, flags=regex.MULTILINE)

def remove_section(text):
    section_regex = r"=+.*?=+"
    return regex.sub(section_regex, "", text)

def remove_categories(text):
    categories_regex = r"^\[\[:*Category:.*?\]\]"
    return regex.sub(categories_regex, "", text,  flags=regex.MULTILINE)

def remove_math(text):
    math_regex = r"<math[^\/]*?>[\S\s]*?<\/math>"
    return regex.sub(math_regex, "", text)

#def _remove_table(self,text):
#    table_regex = r"\{\|([\S\s]*(?!\|\})[\S\s]*|(?R))*?\|\}" #r"\{\|[\S\s]*?(\{\|[\S\s]*?\|\}[\S\s]*?)*?\|\}"
#    return regex.sub(table_regex, "", text)

def remove_sub(text):
    sub_regex = r"<sub[^\/]*?>[\S\s]*?<\/sub>"
    return regex.sub(sub_regex, "", text)
def remove_image(text):
    image_regex = r"\[\[(Image|File):[\S\s]*?(\[\[[\S\s]*?\]\][\S\s]*?)*?\]\]"  # TODO keep caption
    return regex.sub(image_regex, "", text)

def remove_blockquote(text):
    blockquote_regex = r"<blockquote[^\/]*?>[\S\s]*?<\/blockquote>"
    return regex.sub(blockquote_regex, "", text)

def remove_sup(text):
    sup_regex = r"<sup[^\/]*?>[\S\s]*?<\/sup>"
    return regex.sub(sup_regex, "", text)
def remove_small(text):
    small_regex = r"<small[^\/]*?>[\S\s]*?<\/small>"
    return regex.sub(small_regex, "", text)

def remove_big(text):
    big_regex = r"<big[^\/]*?>[\S\s]*?<\/big>"
    return regex.sub(big_regex, "", text)
#def remove_div(text):
#    div_regex = r"<div[^\/]*?>[\S\s]*?<\/div>"
#    return regex.sub(div_regex, "", text)

def remove_empty_parenthesis(text):
    empty_parenthesis = r"\(\s*?\)"
    return regex.sub(empty_parenthesis, "", text)

def remove_see_also(text): # and everything below
    # remove everything that is below the section == References ==
    if text.find("== See also ==") >= 0:
        text = text[:text.find("== See also ==")]
    if text.find("==See also==") >= 0:
        text = text[:text.find("==See also==")]
    return text
