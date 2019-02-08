# -*- coding: utf-8 -*-

import os
import io
import re
import nltk
import warc
import gzip
import time
import errno
import codecs
import cPickle
import datetime
import collections
import multiprocessing

ROOT_CORPUS = "/home/ale/Repositorios/IR-Snippet-Caching/clueweb12/ClueWeb12_"
PATH_RESULTS = "/home/ale/Repositorios/IR-Snippet-Caching/result_list/PL2_2.res"
ROOT_PROCESSED = "/home/ale/Repositorios/IR-Snippet-Caching/processed/ClueWeb12_"
PATH_INDEX = "/home/ale/Repositorios/IR-Snippet-Caching/processed/index"
PATH_STATUS = "/home/ale/Repositorios/IR-Snippet-Caching/processed/status"

# ROOT_CORPUS = "/storage/tolosoft/clueweb12/ClueWeb12_"
# PATH_RESULTS = "/storage/adunogent/analisis/result_list/TF_IDF-100.res"
# ROOT_PROCESSED = "/storage/adunogent/analisis/corpus/ClueWeb12_"
# PATH_INDEX = "/storage/adunogent/analisis/index"
# PATH_STATUS = "/storage/adunogent/analisis/status"

LOCK_READ = multiprocessing.Lock()


class ClueWeb12Parser(object):
    def __init__(self, root_corpus, path_results, root_processed, path_index, path_status):
        self.root_corpus = root_corpus
        self.path_results = path_results
        self.filepath_docs = {}
        self.results_per_id_query = {}
        self.processed_files = set()
        self.document_index = {}
        self.root_processed = root_processed
        self.path_index = path_index
        self.pool = multiprocessing.Pool()
        self.processes = collections.deque()
        self.num_processed_files = 0
        self.path_status = path_status

    def get_document_path(self, id_doc):
        items_id_doc = id_doc.split("-")
        section = items_id_doc[1]
        section_num = section[:2]
        filename = section + "-" + items_id_doc[2] + ".warc.gz"
        path_doc = self.root_corpus + section_num + "/" + section + "/" + filename
        return path_doc

    def load_result_lists(self):
        with codecs.open(self.path_results, mode="r", encoding="utf-8") as file_results:
            for index, line in enumerate(file_results):
                items_result = line.strip().split()
                id_query = int(items_result[0])
                if id_query not in self.results_per_id_query:
                    self.results_per_id_query[id_query] = []
                id_doc = items_result[2]
                self.results_per_id_query[id_query].append(id_doc)
                if id_doc not in self.filepath_docs:
                    filepath_doc = self.get_document_path(id_doc)
                    self.filepath_docs[id_doc] = filepath_doc

    def process_files(self):
        self.read_index()
        for id_query in self.results_per_id_query:
            for id_doc in self.results_per_id_query[id_query]:
                path_file = self.filepath_docs[id_doc]
                if path_file not in self.processed_files and id_doc not in self.document_index:
                    self.processed_files.add(path_file)
                    path_new_file = get_new_document_path(self.root_processed, id_doc)
                    job = self.pool.apply_async(load_file, (path_file, path_new_file))
                    self.processes.append(job)
        self.pool.close()
        self.listen_answers()
        self.write_index()

    def listen_answers(self):
        while len(self.processes) > 0:
            partial_index = self.processes[0].get()
            for id_doc in partial_index:
                self.document_index[id_doc] = partial_index[id_doc]
            self.processes.popleft()
            self.num_processed_files += 1
            if self.num_processed_files % 10 == 0:
                self.write_index()
                self.num_processed_files = 0

    def write_index(self):
        try:
            os.makedirs(os.path.dirname(self.path_index))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        try:
            os.makedirs(os.path.dirname(self.path_status))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        with open(self.path_index, mode="w+") as file_index:
            file_index.write(cPickle.dumps(self.document_index))
        write_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with codecs.open(self.path_status, mode="a", encoding="utf-8") as file_status:
            file_status.write("[" + write_time + "]\n\tINDEX SIZE: " + str(len(self.document_index)) + "\n")
            file_status.write("\tADDED FILES: " + str(self.num_processed_files) + "\n")

    def read_index(self):
        try:
            with open(self.path_index, mode="r") as file_index:
                self.document_index = cPickle.load(file_index)
        except IOError:
            pass


def get_new_document_path(root_processed, id_doc):
    items_id_doc = id_doc.split("-")
    section = items_id_doc[1]
    section_num = section[:2]
    filename = section + "-" + items_id_doc[2] + ".txt"
    path_doc = root_processed + section_num + "/" + section + "/" + filename
    return path_doc


def clean_html(html_doc):
    html_doc = re.sub(r"<script[\s\S]*?</script>|<style[\s\S]*?</style>", "", html_doc, flags=re.IGNORECASE)
    return re.sub(r"<[^>]*?>|&nbsp;", "", html_doc, flags=re.IGNORECASE)


def get_sentences(text_doc):
    all_sentences = []
    sentences = nltk.sent_tokenize(text_doc)
    sentences_tokenized = []
    for sentence in sentences:
        tokens = fast_tokenize(sentence)
        if tokens:
            sentences_tokenized.append(tokens)
    all_sentences.extend(sentences_tokenized)
    return all_sentences


def translate(to_translate):
    tabin = u"áéíóú"
    tabout = u"aeiou"
    tabin = [ord(char) for char in tabin]
    translate_table = dict(zip(tabin, tabout))
    return to_translate.translate(translate_table)


def fast_tokenize(text):
    text = text.lower()
    text = translate(text)
    text = re.sub(u"[^a-zñ]|_", " ", text)
    return text.split()


def load_file(path_file, path_new_file):
    with LOCK_READ:
        print "Subprocess Started Loading: " + path_file
        with open(path_file, "rb") as compressed_file:
            virtual_file = io.BytesIO(compressed_file.read())
    decompressed_file = gzip.GzipFile(fileobj=virtual_file, mode='rb')
    file_doc = warc.WARCFile(fileobj=decompressed_file)
    documents = []
    for record in file_doc:
        if "WARC-TREC-ID" in record:
            id_doc = record["WARC-TREC-ID"]
            document = record.payload.read()
            document = clean_html(document)
            document = unicode(document, "utf-8", errors="ignore")
            document = get_sentences(document)
            documents.append({"id-doc": id_doc, "text": document})
    file_doc.close()
    decompressed_file.close()
    virtual_file.close()
    print "Subprocess Finished Loading: " + path_file
    document_index = write_file(path_new_file, documents)
    return document_index


def write_file(path_file, documents):
    try:
        os.makedirs(os.path.dirname(path_file))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    document_index = {}
    with codecs.open(path_file, mode="w+", encoding="utf-8") as output_file:
        for document in documents:
            id_doc = document["id-doc"]
            output_file.write(id_doc + "\n")
            document_index[id_doc] = {"start": output_file.tell()}
            for sentence in document["text"]:
                output_file.write(",".join(sentence) + "\n")
            document_index[id_doc]["length"] = output_file.tell() - document_index[id_doc]["start"]
    print "Subprocess Finished Writing: " + path_file
    return document_index


if __name__ == "__main__":
    print "ClueWeb12 Preprocessing Started"
    start = time.time()
    clueweb_parser = ClueWeb12Parser(ROOT_CORPUS, PATH_RESULTS, ROOT_PROCESSED, PATH_INDEX, PATH_STATUS)
    clueweb_parser.load_result_lists()
    clueweb_parser.process_files()
    print "ClueWeb12 Preprocessing Finished. Time: " + str(time.time() - start) + "s"
