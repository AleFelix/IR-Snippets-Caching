# -*- coding: utf-8 -*-

import os
import codecs
from datetime import datetime
from document_parser import get_document_path, get_html_doc, clean_html
from document_summarizer import generate_snippet, generate_summary, update_supersnippet, has_good_quality, \
    get_terms_text
import document_summarizer
from cache_manager import DocumentsCache
# from message import Message
# from timer import Timer
import timer
import ConfigParser
import pp

RESULT_LIST_LENGTH = 10
OUTPUT_FILENAME = "snippets_stats"
OUTPUT_EXT = "txt"
CONFIGURATION = "configuration"
FILE_PATH = os.path.abspath(os.path.dirname(__file__))

TASKS = {"LOAD": 0, "DOC": 1, "SURR": 2, "SSNIPP": 3}


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
        self.timer = timer.Timer()
        self.load_times_docs = {}
        self.cache_memory_sizes = sorted(cache_memory_sizes, reverse=True)
        self.statistics = self.create_object_statistics()
        self.dir_output = dir_output
        self.start_datetime = datetime.now()

        self.waiting_ids_docs = []
        self.pending_messages = []
        self.pending_surrogates_analysis = []
        self.pending_ssnippets_analysis = []
        self.waiting_surrogate = False
        self.waiting_ssnippet = False
        self.mpi_buffer = bytearray(1 << 28)
        self.job_server = pp.Server(64)
        self.num_cpus = self.job_server.get_ncpus()
        self.processes_tasks = []

        self.pp_load_doc = pp.Template(self.job_server, worker_load_doc,
                                       (get_document_path, get_html_doc, clean_html),
                                       ("warc", "bs4", "timer"))
        self.pp_analyze_docs = pp.Template(self.job_server, worker_analyze_document,
                                           (has_good_quality, generate_snippet, document_summarizer.summarize_document,
                                            document_summarizer.get_sentences, document_summarizer.get_relevant_terms,
                                            document_summarizer.get_terms_text,
                                            document_summarizer.compute_sentence_relevance,
                                            document_summarizer.compute_sentence_query_relevance),
                                           ("nltk", "collections", "timer", "document_summarizer"))
        self.pp_analyze_surr = pp.Template(self.job_server, worker_analyze_surrogate,
                                           (generate_summary, has_good_quality, generate_snippet,
                                            document_summarizer.summarize_document, document_summarizer.get_sentences,
                                            document_summarizer.get_relevant_terms, document_summarizer.get_terms_text,
                                            document_summarizer.compute_sentence_relevance,
                                            document_summarizer.compute_sentence_query_relevance),
                                           ("nltk", "collections", "timer", "document_summarizer"))
        self.pp_analyze_ssnip = pp.Template(self.job_server, worker_analyze_supersnippet,
                                            (update_supersnippet, has_good_quality, generate_snippet,
                                             document_summarizer.summarize_document, document_summarizer.get_sentences,
                                             document_summarizer.get_relevant_terms, document_summarizer.get_terms_text,
                                             document_summarizer.compute_sentence_relevance,
                                             document_summarizer.compute_sentence_query_relevance),
                                            ("nltk", "collections", "timer", "document_summarizer"))

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

    def start_analysis(self):
        print "STARTING WITH " + str(self.num_cpus) + " PROCESSES"
        self.create_output_dir()
        self.load_stopwords()
        self.more_queries = True
        while self.more_queries:
            self.load_queries()
            self.load_result_lists()
            self.analyze_queries()
        while len(self.pending_messages) > 0 and len(self.pending_surrogates_analysis) > 0\
                and len(self.pending_ssnippets_analysis) > 0:
            self.listen_answers()
        self.statistics["total_time"] = (datetime.now() - self.start_datetime).total_seconds()
        self.write_output_statistics()

    def analyze_queries(self):
        list_ids_queries = [id_query for id_query in self.id_queries]
        current_pos_query = 0
        current_pos_doc = 0
        finished_queries = False
        while not finished_queries:
            print "POS_QUERY: " + str(current_pos_query)
            print "POS_DOC: " + str(current_pos_doc)
            if not finished_queries \
                    and current_pos_doc >= len(self.results_per_id_query[list_ids_queries[current_pos_query]]):
                current_pos_doc = 0
                current_pos_query += 1
                if current_pos_query >= len(list_ids_queries):
                    finished_queries = True
            while not finished_queries and not list_ids_queries[current_pos_query] in self.results_per_id_query:
                current_pos_query += 1
                if current_pos_query >= len(list_ids_queries):
                    finished_queries = True
            if not finished_queries:
                id_query = list_ids_queries[current_pos_query]
                id_doc = self.results_per_id_query[id_query][current_pos_doc]
                current_pos_doc += 1
                loaded = self.process_doc(id_doc, id_query)
                if not loaded:
                    self.send_job(TASKS["LOAD"], id_doc, self.id_queries[id_query])
                else:
                    self.send_job(TASKS["DOC"], id_doc, self.id_queries[id_query], True)
                self.check_pending_analysis()

    def send_job(self, task, id_doc, query=None, was_hit=None, ss_size=None):
        while len(self.processes_tasks) >= self.num_cpus:
            self.listen_answers()
        id_task = len(self.processes_tasks)
        if task == TASKS["LOAD"]:
            self.start_load_doc(id_doc, query, id_task)
        if task == TASKS["DOC"]:
            self.start_analyze_document(id_doc, query, was_hit, id_task)
        if task == TASKS["SURR"]:
            self.start_analyze_surrogate(query, id_doc, id_task)
        if task == TASKS["SSNIPP"]:
            self.start_analyze_supersnippet(query, id_doc, ss_size, id_task)

    def listen_answers(self):
        for id_task, task in enumerate(self.processes_tasks):
            job = task["job"]
            if job.finished:
                job_result = job()
                job_type = task["type"]
                if job_type == TASKS["LOAD"]:
                    id_doc, text_doc, query, load_time = job_result
                    if len(self.waiting_ids_docs) == 0 or id_doc == self.waiting_ids_docs[0]:
                        self.finish_load_doc(id_doc, text_doc, query, load_time)
                        print "CLOSED TASK LOAD FROM " + str(id_task)
                        if len(self.waiting_ids_docs) > 0:
                            self.waiting_ids_docs.pop(0)
                        self.processes_tasks.pop(id_task)
                    else:
                        self.pending_messages.append({"result": job_result, "type": job_type})
                if job_type == TASKS["DOC"]:
                    doc_has_quality, total_time, was_hit, id_doc = job_result
                    self.finish_analyze_document(doc_has_quality, total_time, was_hit, id_doc)
                    print "CLOSED TASK DOC FROM " + str(id_task)
                    self.processes_tasks.pop(id_task)
                if job_type == TASKS["SURR"]:
                    total_time, has_quality, was_hit, id_doc, surrogate = job_result
                    self.finish_analyze_surrogate(total_time, has_quality, was_hit, id_doc, surrogate)
                    print "CLOSED TASK SURR FROM " + str(id_task)
                    self.processes_tasks.pop(id_task)
                if job_type == TASKS["SSNIPP"]:
                    total_time, has_quality, id_doc, ss_size, was_hit, ssnippet = job_result
                    self.finish_analyze_supersnippet(total_time, has_quality, id_doc, ss_size, was_hit, ssnippet)
                    print "CLOSED TASK SSNIPP FROM " + str(id_task)
                    self.processes_tasks.pop(id_task)
        self.check_pending_messages()
        self.check_pending_analysis()

    def check_pending_messages(self):
        check_again = True
        while check_again:
            check_again = False
            for message in self.pending_messages:
                if message["type"] == TASKS["LOAD"]:
                    id_doc, text_doc, query, load_time = message["result"]
                    if self.waiting_ids_docs and id_doc == self.waiting_ids_docs[0]:
                        self.finish_load_doc(id_doc, text_doc, query, load_time)
                        self.waiting_ids_docs.pop(0)
                        check_again = True
                if message["type"] == TASKS["DOC"]:
                    pass
                if message["type"] == TASKS["SURR"]:
                    pass
                if message["type"] == TASKS["SSNIPP"]:
                    pass

    def check_pending_analysis(self):
        while self.pending_surrogates_analysis and self.pending_surrogates_analysis[0]["ready"] \
                and not self.waiting_surrogate:
            id_doc = self.pending_surrogates_analysis[0]["id_doc"]
            id_query = self.pending_surrogates_analysis[0]["id_query"]
            print "SENDING SURROGATE JOB"
            self.pending_surrogates_analysis.pop(0)
            self.send_job(TASKS["SURR"], id_doc, self.id_queries[id_query], True)
        while self.pending_ssnippets_analysis and self.pending_ssnippets_analysis[0]["ready"] \
                and not self.waiting_ssnippet:
            id_doc = self.pending_ssnippets_analysis[0]["id_doc"]
            id_query = self.pending_ssnippets_analysis[0]["id_query"]
            print "SENDING SSNIPPET JOB"
            self.pending_ssnippets_analysis.pop(0)
            self.send_job(TASKS["SSNIPP"], id_doc, self.id_queries[id_query], True, 5)

    def process_doc(self, id_doc, id_query):
        loaded = False
        self.statistics["total_requests"] += 1
        text_doc = self.cache_docs.get_document(id_doc)
        if text_doc is not None:
            loaded = True
        self.pending_surrogates_analysis.append({"ready": loaded, "id_doc": id_doc, "id_query": id_query})
        self.pending_ssnippets_analysis.append({"ready": loaded, "id_doc": id_doc, "id_query": id_query})
        return loaded

    def start_load_doc(self, id_doc, query, id_proc):
        print "SENDING START_LOAD TO " + str(id_proc)
        path_doc = self.filepath_docs[id_doc]
        job = self.pp_load_doc.submit(id_doc, path_doc, query)
        self.processes_tasks.append({"job": job, "type": TASKS["LOAD"]})
        self.waiting_ids_docs.append(id_doc)

    def finish_load_doc(self, id_doc, text_doc, query, load_time):
        self.load_times_docs[id_doc] = load_time
        was_hit = self.cache_docs.get_document(id_doc) is not None
        if not was_hit:
            self.cache_docs.add_document(id_doc, text_doc)
        self.send_job(TASKS["DOC"], id_doc, query, was_hit)
        for surrogate_status in self.pending_surrogates_analysis:
            if surrogate_status["id_doc"] == id_doc:
                surrogate_status["ready"] = True
                break
        for ssnippet_status in self.pending_ssnippets_analysis:
            if ssnippet_status["id_doc"] == id_doc:
                ssnippet_status["ready"] = True
                break

    def start_analyze_document(self, id_doc, query, was_hit, id_proc):
        print "SENDING START_ANALYZE_DOC TO " + str(id_proc)
        text_doc = self.cache_docs.get_document(id_doc)
        job = self.pp_analyze_docs.submit(text_doc, query, self.stopwords, self.snippet_size, was_hit, id_doc)
        self.processes_tasks.append({"job": job, "type": TASKS["DOC"]})

    def finish_analyze_document(self, total_time, doc_has_quality, was_hit, id_doc):
        if was_hit:
            self.update_cache_hits("docs", self.cache_docs)
            self.update_quality_hits("docs", self.cache_docs, doc_has_quality, None)
        else:
            self.update_cache_times("docs", self.cache_docs, self.load_times_docs[id_doc], True)
        self.update_cache_times("docs", self.cache_docs, total_time, False)

    def start_analyze_surrogate(self, query, id_doc, id_proc):
        print "SENDING START_ANALYZE_SURR TO " + str(id_proc)
        surrogate = self.cache_surrogates.get_document(id_doc)
        text_doc = None
        if surrogate is None:
            text_doc = self.cache_docs.get_document(id_doc)
        job = self.pp_analyze_surr.submit(surrogate, id_doc, text_doc, query, self.stopwords, self.snippet_size,
                                          self.surrogate_size)
        self.processes_tasks.append({"job": job, "type": TASKS["SURR"]})
        self.waiting_surrogate = True

    def finish_analyze_surrogate(self, total_time, has_quality, was_hit, id_doc, surrogate):
        if was_hit:
            self.update_cache_hits("surrogates", self.cache_surrogates)
            self.update_quality_hits("surrogates", self.cache_surrogates, has_quality, None)
        else:
            self.update_cache_times("surrogates", self.cache_surrogates,
                                    self.load_times_docs[id_doc] * self.surrogate_size, True)
            self.cache_surrogates.add_document(id_doc, surrogate)
        self.update_cache_times("surrogates", self.cache_surrogates, total_time, False)
        self.waiting_surrogate = False
        for surrogate_status in self.pending_surrogates_analysis:
            if surrogate_status["id_doc"] == id_doc:
                surrogate_status["ready"] = True
                break

    def start_analyze_supersnippet(self, query, id_doc, ss_size, id_proc):
        print "SENDING START_ANALYZE_SS TO " + str(id_proc)
        ssnippet = self.cache_ssnippets[ss_size].get_document(id_doc)
        text_doc = self.cache_docs.get_document(id_doc)
        job = self.pp_analyze_ssnip.submit(ssnippet, id_doc, text_doc, query, self.stopwords, self.snippet_size,
                                           ss_size, self.ssnippet_threshold)
        self.processes_tasks.append({"job": job, "type": TASKS["SSNIPP"]})
        self.waiting_ssnippet = True

    def finish_analyze_supersnippet(self, total_time, has_quality, id_doc, ss_size, was_hit, ssnippet):
        if was_hit:
            self.update_cache_hits("ssnippets", self.cache_ssnippets[ss_size], ss_size)
            self.update_quality_hits("ssnippets", self.cache_ssnippets[ss_size], has_quality, ss_size)
        else:
            self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], self.load_times_docs[id_doc], True,
                                    ss_size)
            self.cache_ssnippets[ss_size].add_document(id_doc, ssnippet)
        self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], total_time, False, ss_size)
        self.waiting_ssnippet = False
        for ssnippet_status in self.pending_ssnippets_analysis:
            if ssnippet_status["id_doc"] == id_doc:
                ssnippet_status["ready"] = True
                break


def worker_load_doc(id_doc, path_doc, query):
    task_timer = timer.Timer()
    task_timer.restart()
    text_doc = get_html_doc(id_doc, path_doc)
    text_doc = clean_html(text_doc)
    task_timer.stop()
    load_time = task_timer.total_time
    return id_doc, text_doc, query, load_time


def worker_analyze_document(text_doc, query, stopwords, snippet_size, was_hit, id_doc):
    task_timer = timer.Timer()
    doc_has_quality = None
    if was_hit:
        doc_has_quality = has_good_quality(text_doc, query, stopwords)
    task_timer.restart()
    generate_snippet(text_doc, stopwords, snippet_size, query)
    task_timer.stop()
    return task_timer.total_time, doc_has_quality, was_hit, id_doc


def worker_analyze_surrogate(surrogate, id_doc, text_doc, query, stopwords, snippet_size, surrogate_size):
    task_timer = timer.Timer()
    surrogate_has_quality = None
    was_hit = surrogate is not None
    if not was_hit:
        surrogate = generate_summary(text_doc, stopwords, surrogate_size)
    else:
        surrogate_has_quality = has_good_quality(surrogate, query, stopwords)
    task_timer.restart()
    generate_snippet(surrogate, stopwords, snippet_size, query)
    task_timer.stop()
    return task_timer.total_time, surrogate_has_quality, was_hit, id_doc, surrogate


def worker_analyze_supersnippet(ssnippet, id_doc, text_doc, query, stopwords, snippet_size, ss_size, ss_threshold):
    task_timer = timer.Timer()
    was_hit = True
    if ssnippet is None:
        was_hit = False
        task_timer.restart()
        snippet = generate_snippet(text_doc, stopwords, snippet_size, query)
        ssnippet = update_supersnippet(ssnippet, snippet, ss_size, ss_threshold, stopwords)
        task_timer.stop()
    task_timer.start()
    snippet = generate_snippet(ssnippet, stopwords, snippet_size, query)
    task_timer.stop()
    has_quality = has_good_quality(snippet, query, stopwords)
    if not has_quality and was_hit:
        task_timer.start()
        snippet = generate_snippet(text_doc, stopwords, snippet_size, query)
        update_supersnippet(ssnippet, snippet, ss_size, ss_threshold, stopwords)
        task_timer.stop()
    return task_timer.total_time, has_quality, id_doc, ss_size, was_hit, ssnippet


def get_config_options(config_parser, options):
    configuration = {}
    for option_name in options:
        configuration[option_name] = config_parser.get(CONFIGURATION, option_name)
    return configuration


def master_main():
    options = ["path_results", "path_queries", "path_stopwords", "root_corpus", "snippet_size", "max_queries",
               "surrogate_size", "ssnippet_sizes", "ssnippet_threshold", "cache_memory_sizes", "dir_output"]
    config_parser = ConfigParser.ConfigParser()
    # try:
    config_parser.readfp(open(FILE_PATH + "/analyzer.conf"))
    configuration = get_config_options(config_parser, options)
    cache_memory_sizes = [int(size) for size in configuration["cache_memory_sizes"].split(",")]
    ssnippet_sizes = [int(size) for size in configuration["ssnippet_sizes"].split(",")]
    snippet_analyzer = SnippetAnalyzer(configuration["path_results"], configuration["path_queries"],
                                       configuration["path_stopwords"], configuration["root_corpus"],
                                       configuration["snippet_size"], configuration["max_queries"],
                                       configuration["surrogate_size"], ssnippet_sizes,
                                       configuration["ssnippet_threshold"], cache_memory_sizes,
                                       configuration["dir_output"])
    snippet_analyzer.start_analysis()
    print snippet_analyzer.statistics
    # except IOError, exception:
    #    print "ERROR: " + str(exception)
    # except ConfigParser.NoSectionError:
    #   print "ERROR: Section [" + CONFIGURATION + "] not found in analyzer.conf"
    # except ConfigParser.NoOptionError, exception:
    #    print "ERROR: " + exception.message


if __name__ == '__main__':
    master_main()
