# -*- coding: utf-8 -*-

from collections import OrderedDict


class DocumentsCache(object):
    def __init__(self, max_documents):
        self.documents = OrderedDict()
        self.max_documents = max_documents

    def get_document(self, id_doc):
        document = self.documents.get(id_doc, None)
        if document is not None:
            self.add_document(id_doc, document)
        return document

    def add_document(self, id_doc, document):
        self.documents.pop(id_doc, None)
        self.documents[id_doc] = document
        if len(self.documents) > self.max_documents:
            self.documents.popitem(last=False)
