# -*- coding: utf-8 -*-

import collections
import gzip


class FileLoader(object):
    def __init__(self, cache_size):
        self.MEGABYTE_MULTIPLIER = 1000000
        self.max_cache_size = cache_size
        self.files = collections.OrderedDict()
        self.current_size = 0

    def get_file(self, path_file):
        if path_file in self.files:
            print "CACHE HIT!!"
            file_data = self.files.pop(path_file)
            self.files[path_file] = file_data
            return self.files[path_file]["data"], self.files[path_file]["index_docs"]
        else:
            print "CACHE MISS!!"
            self.load_file(path_file)
            # with gzip.open(path_file, mode="rb") as file_stream:
            #     self.files[path_file] = file_stream.read()
            #     self.current_size += len(self.files[path_file])
            # while self.current_size >= self.max_cache_size * self.MEGABYTE_MULTIPLIER:
            #     file_data = self.files.popitem(last=False)
            #     self.current_size -= len(file_data)
            return self.files[path_file]["data"], self.files[path_file]["index_docs"]

    def load_file(self, path_file):
        self.files[path_file] = {"data": None, "index_docs": {}}
        found_doc = False
        loading_doc = False
        id_doc = None
        with gzip.open(path_file, mode="rb") as file_stream:
            self.files[path_file]["data"] = file_stream.read()
            file_stream.seek(0)
            for line in file_stream:
                if not found_doc and "WARC-TREC-ID: " in line:
                    found_doc = True
                    id_doc = line.strip().replace("WARC-TREC-ID: ", "")
                elif found_doc and not loading_doc and "Content-Length: " in line:
                    self.files[path_file]["index_docs"][id_doc] = {"start": file_stream.tell(), "end": None}
                    loading_doc = True
                elif loading_doc and "WARC/1.0" in line:
                    self.files[path_file]["index_docs"][id_doc]["end"] = file_stream.tell() - len(line)
                    found_doc = False
                    loading_doc = False
