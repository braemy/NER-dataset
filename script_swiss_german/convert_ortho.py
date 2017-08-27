#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re

inputFile = sys.argv[1]

def convert(s):
    # Case 1
    m = re.search("scht", s)
    if m:
        s = re.sub(r"scht", "st",s)
    # Case 1 capital
    m = re.search("Scht", s)
    if m:
        s = re.sub(r"Scht", "St",s)
    # Case 2
    m = re.search("schp", s)
    if m:
        s = re.sub(r"schp", "sp",s)
    # Case 2 capital
    m = re.search("Schp", s)
    if m:
        s = re.sub(r"Schp", "Sp",s)
    # Case 3
    m = re.search("^gäge", s)
    if m:
        s = 'gegen' + s[m.end():]
    # Case 3 capital
    m = re.search("^Gäge", s)
    if m:
        s = 'Gegen' + s[m.end():]
    # Case 4
    m = re.search("[b-df-hj-np-tv-xz]ä[b-df-hj-np-tv-xz]", s)
    if m:
        s = s[:m.start()+1]+ 'e' + s[m.end()-1:]
    # Case 4 capital
    m = re.search("^Ä[b-df-hj-np-tv-xz]", s)
    if m:
        s = 'E' + s[m.end()-1:]
    # Case 5
    m = re.search("[b-df-hj-np-tv-xz]ü[b-df-hj-np-tv-xz]", s)
    if m:
        s = s[:m.start()+1]+ 'eu' + s[m.end()-1:]
    # Case 6
    m = re.search("^gm[aeiouyäöü]", s)
    if m:
        s = 'gem' + s[m.end()-1:]
    # Case 6 capital
    m = re.search("^Gm[aeiouyäöü]", s)
    if m:
        s = 'Gem' + s[m.end()-1:]
    # Case 7
    m = re.search("^gf[aeiouyäöü]", s)
    if m:
        s = 'gef' + s[m.end()-1:]
    # Case 7 capital
    m = re.search("^Gf[aeiouyäöü]", s)
    if m:
        s = 'Gef' + s[m.end()-1:]
    # Case 8
    m = re.search("^gs[aeiouyäöü]", s)
    if m:
        s = 'ges' + s[m.end()-1:]
    # Case 8 capital
    m = re.search("^Gs[aeiouyäöü]", s)
    if m:
        s = 'Ges' + s[m.end()-1:]
    # Case 9
    m = re.search("^gw[aeiouyäöü]", s)
    if m:
        s = 'gew' + s[m.end()-1:]
    # Case 9 capital
    m = re.search("^Gw[aeiouyäöü]", s)
    if m:
        s = 'Gew' + s[m.end()-1:]
    # Case 10
    m = re.search("^aa[b-df-hj-np-tv-xz]", s)
    if m:
        s = 'an' + s[m.end()-1:]
    # Case 10 capital
    m = re.search("^Aa[b-df-hj-np-tv-xz]", s)
    if m:
        s = 'An' + s[m.end()-1:]
    # Case 11
    m = re.search("[b-df-hj-np-tv-xzB-DF-HJ-NP-TV-XZ]ig$", s)
    if m:
        s = s[:m.start()+1]+ 'ung' #+ s[m.end()-1:]
    # Case 12
    m = re.search("^ii", s)
    if m:
        s = 'ein' + s[m.end():]
    # Case 12 capital
    m = re.search("^Ii", s)
    if m:
        s = 'Ein' + s[m.end():]
    return s


#f = open(inputFile, 'r')

##for line in f:
##    line2 = line.split()[0]
##    line2 = convert(line2)
#    print re.sub("\n", "", line2), re.sub("\n", "", line)
#
#f.close()
