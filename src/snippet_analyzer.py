# -*- coding: utf-8 -*-

import os
import codecs
from datetime import datetime
from document_parser import get_document_path, get_html_doc, clean_html
from document_summarizer import generate_snippet, generate_summary, update_supersnippet, has_good_quality, \
    get_terms_text
from cache_manager import DocumentsCache
from timer import Timer

RESULT_LIST_LENGTH = 10
OUTPUT_FILENAME = "snippets_stats"
OUTPUT_EXT = "txt"


class SnippetAnalyzer(object):
    def __init__(self, path_results, path_queries, path_stopwords, root_corpus, snippet_size, max_queries,
                 surrogate_size, ssnippet_sizes, ssnippet_threshold, cache_memory_sizes, dir_output):
        self.path_results = path_results
        self.path_queries = path_queries
        self.path_stopwords = path_stopwords
        self.root_corpus = root_corpus
        self.snippet_size = snippet_size
        self.max_queries = max_queries
        self.surrogate_size = float(surrogate_size)
        self.ssnippet_sizes = ssnippet_sizes
        self.ssnippet_threshold = ssnippet_threshold
        self.stopwords = None
        self.last_query_line = -1
        self.last_results_line = -1
        self.id_queries = None
        self.results_per_id_query = None
        self.filepath_docs = None
        self.num_of_loaded_queries = None
        self.more_queries = None
        self.cache_docs = DocumentsCache(cache_memory_sizes)
        self.cache_surrogates = DocumentsCache(cache_memory_sizes)
        self.cache_ssnippets = {ss_size: DocumentsCache(cache_memory_sizes) for ss_size in self.ssnippet_sizes}
        self.timer = Timer()
        self.load_times_docs = {}
        self.cache_memory_sizes = sorted(cache_memory_sizes, reverse=True)
        self.statistics = self.create_object_statistics()
        self.dir_output = dir_output
        self.start_datetime = datetime.now()
        self.last_doc_hit = None

    def create_output_dir(self):
        try:
            os.makedirs(self.dir_output)
        except OSError:
            if not os.path.isdir(self.dir_output):
                raise

    def write_output_statistics(self):
        datetime_suffix = self.start_datetime.strftime("-%Y%m%d-%H%M%S")
        stats_filename = OUTPUT_FILENAME + datetime_suffix + "." + OUTPUT_EXT
        path_output_file = os.path.join(self.dir_output, stats_filename)
        with codecs.open(path_output_file, mode="w", encoding="UTF-8") as output_file:
            output_file.write("STATISTICS OF SNIPPETS CACHING METHODS\n")
            for doc_type in ["docs", "surrogates", "ssnippets"]:
                output_file.write("\n{0}:\n".format(doc_type.upper()))
                for mem_size in self.cache_memory_sizes:
                    output_file.write("\t" + str(mem_size) + "MB CACHE:\n")
                    if doc_type in ["docs", "surrogates"]:
                        output_file.write("\t\tTIME: {0:.2f}s\n".format(self.statistics[doc_type]["times"][mem_size]))
                        output_file.write("\t\tHITS: {0} docs\n".format(self.statistics[doc_type]["hits"][mem_size]))
                        output_file.write("\t\tQUALITY MISSES: {0} docs\n"
                                          .format(self.statistics[doc_type]["quality_misses"][mem_size]))
                        output_file.write("\t\tQUALITY HITS: {0} docs\n\n"
                                          .format(self.statistics[doc_type]["quality_hits"][mem_size]))
                    else:
                        for ss_size in self.ssnippet_sizes:
                            output_file.write("\t\tTIME[{0}]: {1:.2f}s\n"
                                              .format(ss_size, self.statistics[doc_type][ss_size]["times"][mem_size]))
                            output_file.write("\t\tHITS[{0}]: {1} docs\n"
                                              .format(ss_size, self.statistics[doc_type][ss_size]["hits"][mem_size]))
                            output_file.write("\t\tQUALITY MISSES[{0}]: {1} docs\n"
                                              .format(ss_size, self.statistics[doc_type][ss_size]["quality_misses"]
                                                                                        [mem_size]))
                            output_file.write("\t\tQUALITY HITS[{0}]: {1} docs\n\n"
                                              .format(ss_size, self.statistics[doc_type][ss_size]["quality_hits"]
                                                                                        [mem_size]))
            output_file.write("\nTOTAL REQUESTS: {0} docs".format(self.statistics["total_requests"]))
            output_file.write("\nTOTAL PROCESSING TIME: {0:.2f}s".format(self.statistics["total_time"]))

    def create_object_statistics(self):
        statistics = {}
        for doc_type in ["docs", "surrogates"]:
            statistics[doc_type] = {
                "times": {mem_size: 0 for mem_size in self.cache_memory_sizes},
                "hits": {mem_size: 0 for mem_size in self.cache_memory_sizes},
                "quality_misses": {mem_size: 0 for mem_size in self.cache_memory_sizes},
                "quality_hits": {mem_size: 0 for mem_size in self.cache_memory_sizes}
            }
        statistics["ssnippets"] = {}
        for ss_size in self.ssnippet_sizes:
            statistics["ssnippets"][ss_size] = {
                "times": {mem_size: 0 for mem_size in self.cache_memory_sizes},
                "hits": {mem_size: 0 for mem_size in self.cache_memory_sizes},
                "quality_misses": {mem_size: 0 for mem_size in self.cache_memory_sizes},
                "quality_hits": {mem_size: 0 for mem_size in self.cache_memory_sizes}
            }
        statistics["total_requests"] = 0
        statistics["total_time"] = 0.0
        return statistics

    def update_cache_times(self, doc_type, cache_type, time, check_hits, ss_size=None):
        if ss_size:
            statistics = self.statistics[doc_type][ss_size]
        else:
            statistics = self.statistics[doc_type]
        statistics["times"][cache_type.max_memory_size] += time
        hits_caches = cache_type.check_hits_extra_caches()
        for mem_size in hits_caches:
            if not check_hits or (check_hits and not hits_caches[mem_size]):
                statistics["times"][mem_size] += time

    def update_cache_hits(self, doc_type, cache_type, ss_size=None):
        if ss_size:
            statistics = self.statistics[doc_type][ss_size]
        else:
            statistics = self.statistics[doc_type]
        statistics["hits"][cache_type.max_memory_size] += 1
        hits_caches = cache_type.check_hits_extra_caches()
        for mem_size in hits_caches:
            if hits_caches[mem_size]:
                statistics["hits"][mem_size] += 1

    def update_quality_hits(self, doc_type, cache_type, has_quality, ss_size=None):
        if ss_size:
            statistics = self.statistics[doc_type][ss_size]
        else:
            statistics = self.statistics[doc_type]
        if has_quality:
            statistics["quality_hits"][cache_type.max_memory_size] += 1
        else:
            statistics["quality_misses"][cache_type.max_memory_size] += 1
        hits_caches = cache_type.check_hits_extra_caches()
        for mem_size in hits_caches:
            if hits_caches[mem_size]:
                if has_quality:
                    statistics["quality_hits"][mem_size] += 1
                else:
                    statistics["quality_misses"][mem_size] += 1

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
                    query = get_terms_text(line.strip(), self.stopwords)
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

    def load_doc(self, id_doc):
        self.statistics["total_requests"] += 1
        text_doc = self.cache_docs.get_document(id_doc)
        if text_doc is None:
            path_doc = self.filepath_docs[id_doc]
            self.timer.restart()
            text_doc = get_html_doc(id_doc, path_doc)
            text_doc = clean_html(text_doc)
            self.timer.stop()
            self.load_times_docs[id_doc] = self.timer.total_time
            self.update_cache_times("docs", self.cache_docs, self.load_times_docs[id_doc], True)
            print "CARGA DOC: " + str(self.timer.total_time)
            self.cache_docs.add_document(id_doc, text_doc)
        else:
            self.update_cache_hits("docs", self.cache_docs)
            self.last_doc_hit = True

    def start_analysis(self):
        self.create_output_dir()
        self.load_stopwords()
        self.more_queries = True
        while self.more_queries:
            self.load_queries()
            self.load_result_lists()
            self.analyze_queries()
        self.statistics["total_time"] = (datetime.now() - self.start_datetime).total_seconds()
        self.write_output_statistics()

    def analyze_queries(self):
        for id_query in self.id_queries:
            print "QUERY °" + str(id_query)
            if id_query in self.results_per_id_query:
                query = self.id_queries[id_query]
                id_docs = self.results_per_id_query[id_query]
                for id_doc in id_docs:
                    self.load_doc(id_doc)
                    self.analyze_document(query, id_doc)
                    self.analyze_surrogate(query, id_doc)
                    for size in self.ssnippet_sizes:
                        self.analyze_supersnippet(query, id_doc, size)

    def analyze_document(self, query, id_doc):
        text_doc = self.cache_docs.get_document(id_doc)
        if self.last_doc_hit:
            doc_has_quality = has_good_quality(text_doc, query, self.stopwords)
            self.update_quality_hits("docs", self.cache_docs, doc_has_quality, None)
            self.last_doc_hit = False
        self.timer.restart()
        generate_snippet(text_doc, self.stopwords, self.snippet_size, query)
        self.timer.stop()
        self.update_cache_times("docs", self.cache_docs, self.timer.total_time, False)
        print "GENERACIÓN SNIPPET: " + str(self.timer.total_time)

    def analyze_surrogate(self, query, id_doc):
        surrogate = self.cache_surrogates.get_document(id_doc)
        if surrogate is None:
            self.update_cache_times("surrogates", self.cache_surrogates,
                                    self.load_times_docs[id_doc] * self.surrogate_size, True)
            text_doc = self.cache_docs.get_document(id_doc)
            surrogate = generate_summary(text_doc, self.stopwords, self.surrogate_size)
            self.cache_surrogates.add_document(id_doc, surrogate)
        else:
            self.update_cache_hits("surrogates", self.cache_surrogates)
            surrogate_has_quality = has_good_quality(surrogate, query, self.stopwords)
            self.update_quality_hits("surrogates", self.cache_surrogates, surrogate_has_quality, None)
        self.timer.restart()
        generate_snippet(surrogate, self.stopwords, self.snippet_size, query)
        self.timer.stop()
        self.update_cache_times("surrogates", self.cache_surrogates, self.timer.total_time, False)
        print "GENERACIÓN SNIPPET S: " + str(self.timer.total_time)

    def analyze_supersnippet(self, query, id_doc, ss_size):
        found = True
        ssnippet = self.cache_ssnippets[ss_size].get_document(id_doc)
        if ssnippet is None:
            found = False
            self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], self.load_times_docs[id_doc], True,
                                    ss_size)
            text_doc = self.cache_docs.get_document(id_doc)
            self.timer.restart()
            snippet = generate_snippet(text_doc, self.stopwords, self.snippet_size, query)
            ssnippet = update_supersnippet(ssnippet, snippet, ss_size, self.ssnippet_threshold,
                                           self.stopwords)
            self.timer.stop()
            self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], self.timer.total_time, False, ss_size)
            self.cache_ssnippets[ss_size].add_document(id_doc, ssnippet)
        else:
            self.update_cache_hits("ssnippets", self.cache_ssnippets[ss_size], ss_size)
        self.timer.restart()
        snippet = generate_snippet(ssnippet, self.stopwords, self.snippet_size, query)
        self.timer.stop()
        self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], self.timer.total_time, False, ss_size)
        print "GENERACIÓN SNIPPET SS: " + str(self.timer.total_time)
        if not has_good_quality(snippet, query, self.stopwords) and found:
            print query
            print "-----------------------------"
            print ssnippet
            self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], self.load_times_docs[id_doc], False,
                                    ss_size)
            self.update_quality_hits("ssnippets", self.cache_ssnippets[ss_size], False, ss_size)
            text_doc = self.cache_docs.get_document(id_doc)
            self.timer.restart()
            snippet = generate_snippet(text_doc, self.stopwords, self.snippet_size, query)
            ssnippet = update_supersnippet(ssnippet, snippet, ss_size, self.ssnippet_threshold,
                                           self.stopwords)
            self.timer.stop()
            self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], self.timer.total_time, False, ss_size)
            print "GENERACIÓN SNIPPET SS Q: " + str(self.timer.total_time)
            self.cache_ssnippets[ss_size].add_document(id_doc, ssnippet)
        elif found:
            self.update_quality_hits("ssnippets", self.cache_ssnippets[ss_size], True, ss_size)
