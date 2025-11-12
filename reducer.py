#!/usr/bin/env python

import sys

current_word = None
current_count = 0
current_document_dict = dict()
word = None

for line in sys.stdin:
    line = line.strip()
    word, value = line.split('\t', 1)
    count, document_number = value.split(';', 1)
    try:
        count = int(count)
    except ValueError:
        continue

    if current_word == word:
        current_count += count
        if document_number in current_document_dict:
            current_document_dict[document_number] += count
        else:
            current_document_dict[document_number] = count
    else:
        if current_word:
            document_list = ["{},{}".format(d, current_document_dict[d]) for d in current_document_dict]
            print("{}\t{}:{}".format(current_word, current_count,';'.join(document_list)))
        current_word = word
        current_count = count
        current_document_dict = dict()
        current_document_dict[document_number] = count

if current_word == word:
    document_list = ["{},{}".format(d, current_document_dict[d]) for d in current_document_dict]
    print("{}\t{}:{}".format(current_word, current_count, ';'.join(document_list)))

