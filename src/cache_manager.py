# -*- coding: utf-8 -*-

import sys
from collections import OrderedDict

MEGABYTE_MULTIPLIER = 1000000


class DocumentsCache(object):
    def __init__(self, max_memory_sizes):
        max_memory_sizes.sort(reverse=True)
        self.documents = OrderedDict()
        self.max_memory_size = max_memory_sizes[0]
        self.memory_size = 0
        self.extra_caches = []
        for memory_size in max_memory_sizes[1:]:
            self.extra_caches.append(ExtraDocumentsCache(memory_size))

    def get_document(self, id_doc):
        document = self.documents.get(id_doc, None)
        if document is not None:
            self.add_document(id_doc, document)
        else:
            for extra_cache in self.extra_caches:
                extra_cache.check_hit(id_doc)
        return document

    def add_document(self, id_doc, document):
        doc_size = sys.getsizeof(document)
        popped_doc = self.documents.pop(id_doc, None)
        if popped_doc is None:
            self.memory_size += doc_size
        self.documents[id_doc] = document
        while self.memory_size > self.max_memory_size * MEGABYTE_MULTIPLIER:
            popped_doc = self.documents.popitem(last=False)
            self.memory_size -= sys.getsizeof(popped_doc)
        for extra_cache in self.extra_caches:
            extra_cache.add_document(id_doc, doc_size)

    def check_hits_extra_caches(self):
        cache_hits = {}
        for extra_cache in self.extra_caches:
            cache_hits[extra_cache.max_memory_size] = extra_cache.last_doc_hit
        return cache_hits

    def get_document_without_updating(self, id_doc):
        return self.documents.get(id_doc, None)


class ExtraDocumentsCache(object):
    def __init__(self, max_memory_size):
        self.max_memory_size = max_memory_size
        self.memory_size = 0
        self.doc_sizes = OrderedDict()
        self.last_doc_hit = None

    def add_document(self, id_doc, doc_size):
        self.last_doc_hit = True
        popped_doc_size = self.doc_sizes.pop(id_doc, None)
        if popped_doc_size is None:
            self.memory_size += doc_size
            self.last_doc_hit = False
        self.doc_sizes[id_doc] = doc_size
        while self.memory_size > self.max_memory_size * MEGABYTE_MULTIPLIER:
            popped_doc_size = self.doc_sizes.popitem(last=False)
            self.memory_size -= sys.getsizeof(popped_doc_size)

    def check_hit(self, id_doc):
        doc_size = self.doc_sizes.get(id_doc, None)
        if doc_size is not None:
            self.add_document(id_doc, doc_size)
        else:
            self.last_doc_hit = False
