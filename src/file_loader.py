# -*- coding: utf-8 -*-

import sys
import collections

MEGABYTE_MULTIPLIER = 1000000


class FileLoader(object):
    def __init__(self, cache_size):
        self.max_cache_size = cache_size * MEGABYTE_MULTIPLIER
        self.files = collections.OrderedDict()
        self.current_size = 0

    def get_file(self, path_file):
        if path_file in self.files:
            file_data = self.files.pop(path_file)
            self.files[path_file] = file_data
            return self.files[path_file]
        else:
            print "CACHE MISS: " + path_file
            self.load_file(path_file)
            return self.files[path_file]

    def load_file(self, path_file):
        with open(path_file, mode="rb") as file_binary_stream:
            self.files[path_file] = file_binary_stream.read()
            self.current_size += sys.getsizeof(self.files[path_file])
        while self.current_size > self.max_cache_size:
            popped_path_file, popped_file = self.files.popitem(last=False)
            self.current_size -= sys.getsizeof(popped_file)
