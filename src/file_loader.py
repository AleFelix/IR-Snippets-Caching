# -*- coding: utf-8 -*-

import collections


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
            return self.files[path_file]
        else:
            print "CACHE MISS!!"
            self.load_file(path_file)
            return self.files[path_file]

    def load_file(self, path_file):
        with open(path_file, mode="rb") as file_binary_stream:
            self.files[path_file] = file_binary_stream.read()
