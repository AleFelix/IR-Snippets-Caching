# -*- coding: utf-8 -*-

import sys
from collections import OrderedDict

MEGABYTE_MULTIPLIER = 1000000


class DocumentsCache(object):
    def __init__(self, max_memory_size):
        self.documents = OrderedDict()
        self.max_memory_size = max_memory_size * MEGABYTE_MULTIPLIER
        self.memory_size = 0

    def get_document(self, id_doc):
        document = self.documents.get(id_doc, None)
        if document is not None:
            self.add_document(id_doc, document)
        return document

    def add_document(self, id_doc, document):
        popped_doc = self.documents.pop(id_doc, None)
        if popped_doc is None:
            self.memory_size += sys.getsizeof(document)
        self.documents[id_doc] = document
        while self.memory_size > self.max_memory_size:
            popped_doc = self.documents.popitem(last=False)
            self.memory_size -= sys.getsizeof(popped_doc)
