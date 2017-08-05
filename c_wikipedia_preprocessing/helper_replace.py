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

def replace_list_bullets(text):
    list_regex = r"^(\*+ ?).*?"
    result = regex.finditer(list_regex, text, flags=regex.MULTILINE)
    output = text
    for r in result:
        output = output.replace(text[r.start():r.end()], "", 1)
    return output

def replace_div(text):
    reg = r"<div[^\/]*?>([\S\s]*?)<\/div>"
    result = regex.finditer(reg, text)
    output = text
    for r in result:
        output = output.replace(text[r.start():r.end()], r.group(1), 1)
    return output

def clean_table(text):
    reg = r"^ *\{\|.*" #{| class
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r"^ *\|\}"  # |}
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r"^----"  # |}
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    #
    reg = r"^!.*|" #table header ! style="width:65%;"| Winning seasons
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r"\|?.*(style|align|width)=[^|\n]*\|?" #|valign="top"|
    text = regex.sub(reg, "", text)
    reg = r"\|?.*(bgcolor|rowspan|colspan)=[^|\n]*\|?"  # |valign="top"|
    text = regex.sub(reg, "", text)
    reg = r"^\|-" #|-
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r"^\| ?" #| fjdijsaf
    text = regex.sub(reg, "", text, flags=regex.MULTILINE)
    reg = r" *\|\| *" # 1 || [[Real Madrid C.F.|Real Madrid]] || 85 ||
    text = regex.sub(reg, " ", text)

    return text