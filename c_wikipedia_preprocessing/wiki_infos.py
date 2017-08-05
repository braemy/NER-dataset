import regex

def is_disambiguation(text):
    reg = r"{{(D|d)isambiguation\|.*}}"
    if regex.search(reg, text):
        return True
    else:
        return False