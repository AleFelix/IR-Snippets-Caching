# -*- coding: utf-8 -*-

import codecs
from document_parser import get_document_path, get_html_doc, clean_html
from document_summarizer import generate_snippet, generate_summary
from cache_manager import DocumentsCache
from timer import Timer

RESULT_LIST_LENGTH = 10


class SnippetAnalyzer(object):
    def __init__(self, path_results, path_queries, path_stopwords, root_corpus, snippet_size, max_queries,
                 surrogate_size, ssnippet_size, ssnippet_threshold, cache_size):
        self.path_results = path_results
        self.path_queries = path_queries
        self.path_stopwords = path_stopwords
        self.root_corpus = root_corpus
        self.snippet_size = snippet_size
        self.max_queries = max_queries
        self.surrogate_size = surrogate_size
        self.ssnippet_size = ssnippet_size
        self.ssnippet_threshold = ssnippet_threshold
        self.stopwords = None
        self.last_query_line = -1
        self.last_results_line = -1
        self.id_queries = None
        self.results_per_id_query = None
        self.filepath_docs = None
        self.num_of_loaded_queries = None
        self.more_queries = None
        self.cache_docs = DocumentsCache(cache_size)
        self.cache_surrogates = DocumentsCache(cache_size)
        self.timer = Timer()

    def load_stopwords(self):
        self.stopwords = []
        with codecs.open(self.path_stopwords, mode="r", encoding="utf-8") as file_stopwords:
            for line in file_stopwords:
                self.stopwords.append(line.strip())

    def load_queries(self):
        with codecs.open(self.path_queries, mode="r", encoding="utf-8") as file_queries:
            self.num_of_loaded_queries = 0
            self.id_queries = {}
            self.more_queries = False
            for index, line in enumerate(file_queries):
                if index > self.last_query_line:
                    query = line.strip().split()
                    self.id_queries[index + 1] = query
                    self.last_query_line += 1
                    self.num_of_loaded_queries += 1
                if self.num_of_loaded_queries == self.max_queries:
                    self.more_queries = True
                    break

    def load_result_lists(self):
        self.results_per_id_query = {}
        self.filepath_docs = {}
        num_of_loaded_results = 0
        max_results = self.num_of_loaded_queries * RESULT_LIST_LENGTH
        with codecs.open(self.path_results, mode="r", encoding="utf-8") as file_results:
            for index, line in enumerate(file_results):
                if index > self.last_results_line:
                    items_result = line.strip().split()
                    id_query = int(items_result[0])
                    if id_query not in self.results_per_id_query:
                        self.results_per_id_query[id_query] = []
                    id_doc = items_result[2]
                    self.results_per_id_query[id_query].append(id_doc)
                    if id_doc not in self.filepath_docs:
                        filepath_doc = get_document_path(self.root_corpus, id_doc)
                        self.filepath_docs[id_doc] = filepath_doc
                    num_of_loaded_results += 1
                    self.last_results_line += 1
                if num_of_loaded_results == max_results:
                    break

    def start_analysis(self):
        self.load_stopwords()
        self.more_queries = True
        while self.more_queries:
            self.load_queries()
            self.load_result_lists()
            self.analyze_queries()

    def analyze_queries(self):
        for id_query in self.id_queries:
            if id_query in self.results_per_id_query:
                query = self.id_queries[id_query]
                id_docs = self.results_per_id_query[id_query]
                for id_doc in id_docs:
                    self.analyze_document(query, id_doc)
                    self.analyze_surrogate(query, id_doc)

    def analyze_document(self, query, id_doc):
        self.timer.reset()
        self.timer.start()
        text_doc = self.cache_docs.get_document(id_doc)
        if text_doc is None:
            path_doc = self.filepath_docs[id_doc]
            text_doc = clean_html(get_html_doc(id_doc, path_doc))
            self.cache_docs.add_document(id_doc, text_doc)
        generate_snippet(text_doc, self.stopwords, self.snippet_size, query)
        self.timer.stop()

    def analyze_surrogate(self, query, id_doc):
        self.timer.reset()
        self.timer.start()
        surrogate = self.cache_surrogates.get_document(id_doc)
        if surrogate is None:
            path_doc = self.filepath_docs[id_doc]
            text_doc = clean_html(get_html_doc(id_doc, path_doc))
            self.timer.stop()
            surrogate = generate_summary(text_doc, self.stopwords, self.surrogate_size)
            self.timer.start()
        generate_snippet(surrogate, self.stopwords, self.snippet_size, query)
        self.timer.stop()

    def analyze_supersnippet(self, query, id_doc):
        pass
