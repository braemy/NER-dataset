


import sys
sys.path.append('/home/braemy/wiki/opennlp-python/')

import opennlp
instance = opennlp.OpenNLP("/home/braemy/wiki/opennlp", "SentenceDetector", "en-sent.bin")

tokens = instance.parse("'''Anarchism''' is a [[political philosophy]] that advocates [[self-governance|self-governed]] societies based on voluntary institutions.")