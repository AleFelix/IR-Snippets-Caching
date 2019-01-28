# -*- coding: utf-8 -*-

import sys
import gzip
from collections import OrderedDict

PATH_DOC = "/home/ale/Repositorios/IR-Snippet-Caching/clueweb12/ClueWeb12_00/0000wb/0000wb-00.warc.gz"


def index_warc(path_doc):
    found_doc = False
    id_doc = None
    index_positions = OrderedDict()
    with gzip.open(path_doc, mode="rb") as file_stream:
        for line in file_stream:
            if not found_doc and "WARC-TREC-ID: " in line:
                found_doc = True
                id_doc = line.strip().replace("WARC-TREC-ID: ", "")
            elif found_doc and "Content-Length: " in line:
                index_positions[id_doc] = file_stream.tell()
                found_doc = False
    for id_doc in index_positions:
        print id_doc + ": " + str(index_positions[id_doc])


def load_file(path_file):
    files = {path_file: {"data": "", "index_docs": {}}}
    found_doc = False
    loading_doc = False
    id_doc = None
    with gzip.open(path_file, mode="rb") as file_stream:
        files[path_file]["data"] = file_stream.read()
        file_stream.seek(0)
        for line in file_stream:
            if not found_doc and "WARC-TREC-ID: " in line:
                found_doc = True
                id_doc = line.strip().replace("WARC-TREC-ID: ", "")
            elif found_doc and not loading_doc and "Content-Length: " in line:
                files[path_file]["index_docs"][id_doc] = {"start": file_stream.tell(), "end": None}
                loading_doc = True
            elif loading_doc and "WARC/1.0" in line:
                files[path_file]["index_docs"][id_doc]["end"] = file_stream.tell() - len(line)
                found_doc = False
                loading_doc = False
    for path_file in files:
        for id_doc in files[path_file]["index_docs"]:
            print id_doc + ": START: " + str(files[path_file]["index_docs"][id_doc]["start"]) + " END: " + \
                  str(files[path_file]["index_docs"][id_doc]["end"])


if __name__ == "__main__":
    load_file(PATH_DOC)
