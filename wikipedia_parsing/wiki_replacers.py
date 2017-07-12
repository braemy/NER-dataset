import regex


def replace_code(text):
    reg = r"<code>(.*?)<\/code>"  # <code>23456789-</code>
    result = regex.finditer(reg, text)
    output = text
    for r in result:
        output = output.replace(text[r.start():r.end()], r.group(1), 1)
    return output


def replace_tt(text):
    reg = r"<tt>([^<]*?)<\/tt>"  # <tt>SubBytes</tt>
    result = regex.finditer(reg, text)
    output = text
    for r in result:
        output = output.replace(text[r.start():r.end()], r.group(1), 1)
    return output


def replace_s(text):
    reg = r"<s>(.*)?<\/s>"  # <s>TAPE</s>
    result = regex.finditer(reg, text)
    output = text
    for r in result:
        output = output.replace(text[r.start():r.end()], r.group(1), 1)
    return output


def replace_wikt(text):
    reg = r"\[\[:?(wikt|wiktionary):([\S\s]*?)\|([\S\s]*?)\]\]"
    result = regex.finditer(reg, text)
    output = text
    for r in result:
        output = output.replace(text[r.start():r.end()], r.group(3), 1)
    return output

def replace_lang(text):
    reg =  r"('')?\{\{(lang|transl)\|.*?\|(.*?)\}\}('')?"  #''{{lang|fr|etats-unien}}''
    result = regex.finditer(reg, text)
    output = text
    for r in result:
        output = output.replace(text[r.start():r.end()], r.group(3), 1)
    return output

def replace_html_tags(text):
    text =  text.replace("&nbsp;", " ")
    text =  text.replace("&ndash;", "--")
    text =  text.replace("&mdash;", "---")
    text =  text.replace("&amp;", "&")
    return text
