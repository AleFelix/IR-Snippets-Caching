# -*- coding: utf-8 -*-

import os
import codecs
from datetime import datetime
from document_parser import get_document_path, get_tokens_doc_with_seek
from document_summarizer import generate_snippet, generate_summary, update_supersnippet, has_good_quality, \
    get_terms_text
from cache_manager import DocumentsCache
import timer
import traceback
# from file_loader import FileLoader
import multiprocessing
import cProfile
import cPickle
import pudb

RESULT_LIST_LENGTH = 10
OUTPUT_FILENAME = "snippets_stats"
OUTPUT_EXT = "txt"

TASKS = {"DOC": 0, "SURR": 1, "SSNIPP": 2}

DEBUG = False


class SnippetAnalyzer(object):
    def __init__(self, path_results, path_queries, path_stopwords, path_index, root_corpus, snippet_size, max_queries,
                 surrogate_size, ssnippet_sizes, ssnippet_threshold, cache_memory_sizes, dir_output, file_cache_size,
                 training_limit):
        self.path_results = path_results
        self.path_queries = path_queries
        self.path_stopwords = path_stopwords
        self.path_index = path_index
        self.root_corpus = root_corpus
        self.snippet_size = int(snippet_size)
        self.max_queries = int(max_queries)
        self.surrogate_size = float(surrogate_size)
        self.ssnippet_sizes = ssnippet_sizes
        self.ssnippet_threshold = float(ssnippet_threshold)
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

        self.num_cpus = multiprocessing.cpu_count()
        self.processes_tasks = {}
        # self.file_loader = FileLoader(int(file_cache_size))
        self.docs_index = {}
        self.training_limit = int(training_limit)
        self.processed_queries = None
        self.training_mode = None
        self.last_process_id = 0
        self.results_queue = multiprocessing.Queue()

    def create_output_dir(self):
        try:
            os.makedirs(self.dir_output)
        except OSError:
            if not os.path.isdir(self.dir_output):
                raise

    def write_output_statistics(self, num_processed_queries=None):
        datetime_suffix = self.start_datetime.strftime("-%Y%m%d-%H%M%S")
        if num_processed_queries is None:
            stats_filename = OUTPUT_FILENAME + datetime_suffix + "." + OUTPUT_EXT
        else:
            stats_filename = OUTPUT_FILENAME + datetime_suffix + "." + OUTPUT_EXT + "_" + str(num_processed_queries)
        path_output_file = os.path.join(self.dir_output, stats_filename)
        with codecs.open(path_output_file, mode="w", encoding="UTF-8") as output_file:
            output_file.write("STATISTICS OF SNIPPETS CACHING METHODS\n")
            for doc_type in ["docs", "surrogates", "ssnippets"]:
                output_file.write("\n{0}:\n".format(doc_type.upper()))
                for mem_size in self.cache_memory_sizes:
                    output_file.write("\t" + str(mem_size) + "MB CACHE:\n")
                    if doc_type in ["docs", "surrogates"]:
                        output_file.write("\t\tTIME: {0:.6f}s\n".format(self.statistics[doc_type]["times"][mem_size]))
                        output_file.write("\t\tHITS: {0} docs\n".format(self.statistics[doc_type]["hits"][mem_size]))
                        output_file.write("\t\tQUALITY MISSES: {0} docs\n"
                                          .format(self.statistics[doc_type]["quality_misses"][mem_size]))
                        output_file.write("\t\tQUALITY HITS: {0} docs\n\n"
                                          .format(self.statistics[doc_type]["quality_hits"][mem_size]))
                    else:
                        for ss_size in self.ssnippet_sizes:
                            output_file.write("\t\tTIME[{0}]: {1:.6f}s\n"
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

    def update_cache_times(self, doc_type, cache_type, update_max, time, check_hits=None, ss_size=None,
                           hits_caches=None):
        if not self.training_mode:
            if ss_size:
                statistics = self.statistics[doc_type][ss_size]
            else:
                statistics = self.statistics[doc_type]
            if update_max:
                statistics["times"][cache_type.max_memory_size] += time
            if hits_caches is None:
                hits_caches = cache_type.check_hits_extra_caches()
                if doc_type == "surrogates":
                    pass
            for mem_size in hits_caches:
                if not check_hits or (check_hits and not hits_caches[mem_size]):
                    statistics["times"][mem_size] += time

    def update_cache_hits(self, doc_type, cache_type, update_max, ss_size=None, hits_caches=None):
        if not self.training_mode:
            if ss_size:
                statistics = self.statistics[doc_type][ss_size]
            else:
                statistics = self.statistics[doc_type]
            if update_max:
                statistics["hits"][cache_type.max_memory_size] += 1
            if hits_caches is None:
                hits_caches = cache_type.check_hits_extra_caches()
            for mem_size in hits_caches:
                if hits_caches[mem_size]:
                    statistics["hits"][mem_size] += 1

    def update_quality_hits(self, doc_type, cache_type, update_max, has_quality, ss_size=None, hits_caches=None):
        if not self.training_mode:
            if ss_size:
                statistics = self.statistics[doc_type][ss_size]
            else:
                statistics = self.statistics[doc_type]
            if update_max:
                if has_quality:
                    statistics["quality_hits"][cache_type.max_memory_size] += 1
                else:
                    statistics["quality_misses"][cache_type.max_memory_size] += 1
            if hits_caches is None:
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

    def load_index(self):
        with open(self.path_index, mode="rb") as file_index:
            self.docs_index = cPickle.load(file_index)

    def load_queries(self):
        with codecs.open(self.path_queries, mode="r", encoding="utf-8") as file_queries:
            self.num_of_loaded_queries = 0
            self.id_queries = {}
            self.more_queries = False
            for index, line in enumerate(file_queries):
                if index > self.last_query_line:
                    query = list(get_terms_text(line.strip(), self.stopwords))
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
        self.load_index()
        self.more_queries = True
        self.processed_queries = 0
        self.training_mode = self.training_limit > 0
        while self.more_queries:
            self.load_queries()
            self.load_result_lists()
            self.analyze_queries()
        self.listen_answers()
        self.statistics["total_time"] = (datetime.now() - self.start_datetime).total_seconds()
        self.write_output_statistics()

    def analyze_queries(self):
        list_ids_queries = [id_query for id_query in self.id_queries]
        current_pos_query = 0
        current_pos_doc = 0
        finished_queries = False
        while not finished_queries:
            # print "POS_QUERY: " + str(current_pos_query)
            # print "POS_DOC: " + str(current_pos_doc)
            if not finished_queries \
                    and current_pos_doc >= len(self.results_per_id_query[list_ids_queries[current_pos_query]]):
                self.listen_answers()
                current_pos_doc = 0
                current_pos_query += 1
                print "Processed Query #" + str(self.processed_queries)
                self.processed_queries += 1
                if self.training_mode and self.processed_queries >= self.training_limit:
                    self.training_mode = False
                if not self.training_mode and self.processed_queries % 500 == 0:
                    print "Writing Partial Statistics to Disk"
                    self.statistics["total_time"] = (datetime.now() - self.start_datetime).total_seconds()
                    self.write_output_statistics(self.processed_queries)
                if current_pos_query >= len(list_ids_queries):
                    finished_queries = True
            while not finished_queries and not list_ids_queries[current_pos_query] in self.results_per_id_query:
                current_pos_query += 1
                print "Processed Query #" + str(self.processed_queries)
                self.processed_queries += 1
                if self.training_mode and self.processed_queries >= self.training_limit:
                    self.training_mode = False
                if not self.training_mode and self.processed_queries % 500 == 0:
                    print "Writing Partial Statistics to Disk"
                    self.statistics["total_time"] = (datetime.now() - self.start_datetime).total_seconds()
                    self.write_output_statistics(self.processed_queries)
                if current_pos_query >= len(list_ids_queries):
                    finished_queries = True
            if not finished_queries:
                id_query = list_ids_queries[current_pos_query]
                id_doc = self.results_per_id_query[id_query][current_pos_doc]
                current_pos_doc += 1
                loaded, extra_hits = self.check_loaded_doc(id_doc)
                if not loaded:
                    # file_data = self.file_loader.get_file(self.filepath_docs[id_doc])
                    self.load_doc(id_doc, self.filepath_docs[id_doc], self.docs_index[id_doc])
                self.send_job(TASKS["DOC"], id_doc, self.id_queries[id_query], was_hit=loaded, extra_hits=extra_hits)
                self.send_job(TASKS["SURR"], id_doc, self.id_queries[id_query])
                for ss_size in self.ssnippet_sizes:
                    self.send_job(TASKS["SSNIPP"], id_doc, self.id_queries[id_query], ss_size=ss_size)

    def send_job(self, task, id_doc, query=None, was_hit=None, extra_hits=None, ss_size=None):
        if task == TASKS["DOC"]:
            self.start_analyze_document(id_doc, query, was_hit, extra_hits)
        if task == TASKS["SURR"]:
            self.start_analyze_surrogate(query, id_doc)
        if task == TASKS["SSNIPP"]:
            self.start_analyze_supersnippet(query, id_doc, ss_size)

    def listen_answers(self):
        for _ in self.processes_tasks:
            try:
                job_result = self.results_queue.get(timeout=120)
            except Exception as ex:
                print "An Exception ocurred while waiting for a Process: " + str(ex)
                traceback.print_exc()
                pudb.set_trace()
                job_result = None
            if job_result is not None:
                if job_result["type"] == TASKS["DOC"]:
                    doc_has_quality, total_time, was_hit, extra_hits, id_doc = job_result["result"]
                    self.finish_analyze_document(doc_has_quality, total_time, was_hit, extra_hits, id_doc)
                    # print "CLOSED TASK DOC"
                if job_result["type"] == TASKS["SURR"]:
                    total_time, has_quality, id_doc, surrogate = job_result["result"]
                    self.finish_analyze_surrogate(total_time, has_quality, id_doc, surrogate)
                    # print "CLOSED TASK SURR"
                if job_result["type"] == TASKS["SSNIPP"]:
                    total_time, has_quality, id_doc, ss_size, ssnippet = job_result["result"]
                    self.finish_analyze_supersnippet(total_time, has_quality, id_doc, ss_size, ssnippet)
                    # print "CLOSED TASK SSNIPP"
        for id_proc in self.processes_tasks.keys():
            self.processes_tasks[id_proc].join(timeout=120)
            self.processes_tasks.pop(id_proc)
        self.results_queue.close()
        self.results_queue = multiprocessing.Queue()

    def check_loaded_doc(self, id_doc):
        self.statistics["total_requests"] += 1
        text_doc = self.cache_docs.get_document(id_doc)
        loaded = text_doc is not None
        extra_hits = self.cache_docs.check_hits_extra_caches()
        return loaded, extra_hits

    def load_doc(self, id_doc, file_path, index_doc):
        task_timer = timer.Timer()
        task_timer.restart()
        text_doc = get_tokens_doc_with_seek(file_path, index_doc)
        task_timer.stop()
        self.load_times_docs[id_doc] = task_timer.total_time
        self.cache_docs.add_document(id_doc, text_doc)

    def start_analyze_document(self, id_doc, query, was_hit, extra_hits):
        # print "SENDING START_ANALYZE_DOC TO " + str(id_proc)
        text_doc = self.cache_docs.get_document(id_doc)
        if DEBUG:
            job = multiprocessing.Process(target=profile_wad, args=(text_doc, query, self.stopwords, self.snippet_size,
                                          was_hit, extra_hits, id_doc, self.results_queue))
        else:
            job = multiprocessing.Process(target=worker_analyze_document, args=(text_doc, query, self.stopwords,
                                          self.snippet_size, was_hit, extra_hits, id_doc, self.results_queue))
        job.start()
        self.processes_tasks[self.last_process_id] = job
        self.last_process_id += 1

    def finish_analyze_document(self, total_time, doc_has_quality, was_hit, extra_hits, id_doc):
        self.update_cache_hits("docs", self.cache_docs, was_hit, hits_caches=extra_hits)
        self.update_quality_hits("docs", self.cache_docs, was_hit, doc_has_quality, hits_caches=extra_hits)
        self.update_cache_times("docs", self.cache_docs, not was_hit, self.load_times_docs[id_doc], check_hits=True,
                                hits_caches=extra_hits)
        self.update_cache_times("docs", self.cache_docs, True, total_time, check_hits=False, hits_caches=extra_hits)

    def start_analyze_surrogate(self, query, id_doc):
        # print "SENDING START_ANALYZE_SURR TO " + str(id_proc)
        surrogate = self.cache_surrogates.get_document(id_doc)
        text_doc = None
        if surrogate is None:
            text_doc = self.cache_docs.get_document_without_updating(id_doc)
        if DEBUG:
            job = multiprocessing.Process(target=profile_was, args=(surrogate, id_doc, text_doc, query, self.stopwords,
                                          self.snippet_size, self.surrogate_size, self.results_queue))
        else:
            job = multiprocessing.Process(target=worker_analyze_surrogate, args=(surrogate, id_doc, text_doc, query,
                                          self.stopwords, self.snippet_size, self.surrogate_size, self.results_queue))
        job.start()
        self.processes_tasks[self.last_process_id] = job
        self.last_process_id += 1

    def finish_analyze_surrogate(self, total_time, has_quality, id_doc, surrogate):
        cached_surrogate = self.cache_surrogates.get_document(id_doc)
        was_hit = (cached_surrogate is not None)
        self.update_cache_hits("surrogates", self.cache_surrogates, was_hit)
        self.update_quality_hits("surrogates", self.cache_surrogates, was_hit, has_quality)
        load_time_surrogate = self.load_times_docs[id_doc] * self.surrogate_size
        self.update_cache_times("surrogates", self.cache_surrogates, not was_hit, load_time_surrogate, check_hits=True)
        self.update_cache_times("surrogates", self.cache_surrogates, True, total_time, check_hits=False)
        self.cache_surrogates.add_document(id_doc, surrogate)

    def start_analyze_supersnippet(self, query, id_doc, ss_size):
        # print "SENDING START_ANALYZE_SS TO " + str(id_proc)
        ssnippet = self.cache_ssnippets[ss_size].get_document(id_doc)
        text_doc = self.cache_docs.get_document_without_updating(id_doc)
        if DEBUG:
            job = multiprocessing.Process(target=profile_wss, args=(ssnippet, id_doc, text_doc, query, self.stopwords,
                                          self.snippet_size, ss_size, self.ssnippet_threshold, self.results_queue))
        else:
            job = multiprocessing.Process(target=worker_analyze_supersnippet, args=(ssnippet, id_doc, text_doc, query,
                                          self.stopwords, self.snippet_size, ss_size, self.ssnippet_threshold,
                                          self.results_queue))
        job.start()
        self.processes_tasks[self.last_process_id] = job
        self.last_process_id += 1

    def finish_analyze_supersnippet(self, total_time, has_quality, id_doc, ss_size, ssnippet):
        cached_ssnippet = self.cache_ssnippets[ss_size].get_document(id_doc)
        was_hit = (cached_ssnippet is not None)
        self.update_cache_hits("ssnippets", self.cache_ssnippets[ss_size], was_hit, ss_size=ss_size)
        self.update_quality_hits("ssnippets", self.cache_ssnippets[ss_size], was_hit, has_quality, ss_size=ss_size)
        self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], not was_hit, self.load_times_docs[id_doc],
                                check_hits=True, ss_size=ss_size)
        self.update_cache_times("ssnippets", self.cache_ssnippets[ss_size], True, total_time, check_hits=False,
                                ss_size=ss_size)
        self.cache_ssnippets[ss_size].add_document(id_doc, ssnippet)


def profile_wad(text_doc, query, stopwords, snippet_size, was_hit, extra_hits, id_doc, results_queue, id_proc):
    cProfile.runctx("worker_analyze_document(text_doc, query, stopwords, snippet_size, was_hit, extra_hits, id_doc)",
                    globals(), locals(), "profiling/profile_wad-%d.out" % id_proc)


def profile_was(surrogate, id_doc, text_doc, query, stopwords, snippet_size, surrogate_size, results_queue, id_proc):
    cProfile.runctx("worker_analyze_surrogate(surrogate, id_doc, text_doc, query, stopwords, snippet_size,"
                    "surrogate_size)", globals(), locals(),
                    "profiling/profile_was-%d.out" % id_proc)


def profile_wss(ssnippet, id_doc, text_doc, query, stopwords, snippet_size, ss_size, ss_threshold, results_queue,
                id_proc):
    cProfile.runctx("worker_analyze_supersnippet(ssnippet, id_doc, text_doc, query, stopwords, snippet_size, ss_size,"
                    "ss_threshold)", globals(), locals(),
                    "profiling/profile_wss-%d.out" % id_proc)


def worker_analyze_document(text_doc, query, stopwords, snippet_size, was_hit, extra_hits, id_doc, results_queue):
    try:
        task_timer = timer.Timer()
        doc_has_quality = None
        if was_hit:
            doc_has_quality = has_good_quality(text_doc, query, stopwords)
        task_timer.restart()
        generate_snippet(text_doc, stopwords, snippet_size, query)
        task_timer.stop()
        # print "WORKER: DOC TIME: " + str(task_timer.total_time)
        results_queue.put({"type": TASKS["DOC"], "result": (task_timer.total_time, doc_has_quality, was_hit, extra_hits,
                          id_doc)})
    except Exception as ex:
        print ex
        traceback.print_exc()


def worker_analyze_surrogate(surrogate, id_doc, text_doc, query, stopwords, snippet_size, surrogate_size,
                             results_queue):
    try:
        task_timer = timer.Timer()
        if surrogate is None:
            surrogate = generate_summary(text_doc, stopwords, surrogate_size)
        surrogate_has_quality = has_good_quality(surrogate, query, stopwords)
        task_timer.restart()
        generate_snippet(surrogate, stopwords, snippet_size, query)
        task_timer.stop()
        # print "WORKER: SURR TIME: " + str(task_timer.total_time)
        results_queue.put({"type": TASKS["SURR"], "result": (task_timer.total_time, surrogate_has_quality, id_doc,
                          surrogate)})
    except Exception as ex:
        print ex
        traceback.print_exc()


def worker_analyze_supersnippet(ssnippet, id_doc, text_doc, query, stopwords, snippet_size, ss_size, ss_threshold,
                                results_queue):
    try:
        task_timer = timer.Timer()
        was_hit = ssnippet is not None
        if not was_hit:
            task_timer.restart()
            snippet = generate_snippet(text_doc, stopwords, ss_size, query)
            ssnippet = update_supersnippet(ssnippet, snippet, ss_size, ss_threshold, stopwords)
            task_timer.stop()
        task_timer.start()
        snippet = generate_snippet(ssnippet, stopwords, snippet_size, query)
        task_timer.stop()
        has_quality = has_good_quality(snippet, query, stopwords)
        if not has_quality and was_hit:
            task_timer.start()
            snippet = generate_snippet(text_doc, stopwords, ss_size, query)
            ssnippet = update_supersnippet(ssnippet, snippet, ss_size, ss_threshold, stopwords)
            generate_snippet(ssnippet, stopwords, snippet_size, query)
            task_timer.stop()
        # print "WORKER: SSNIP TIME: " + str(task_timer.total_time)
        results_queue.put({"type": TASKS["SSNIPP"], "result": (task_timer.total_time, has_quality, id_doc, ss_size,
                          ssnippet)})
    except Exception as ex:
        print ex
        traceback.print_exc()
