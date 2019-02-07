# -*- coding: utf-8 -*-

import os
import codecs

PATH_AOL = "/home/ale/Repositorios/IR-Snippet-Caching/split-AOL"
PATH_JOINED = "/home/ale/Repositorios/IR-Snippet-Caching/split-AOL/AOL-100000.txt"


def join_aol(path_aol, path_joined):
    results = []
    for file_name in sorted(os.listdir(path_aol)):
        path_file = os.path.join(path_aol, file_name)
        with codecs.open(path_file, mode="r", encoding="utf-8") as results_file:
            for line in results_file:
                results.append(line)
    with codecs.open(path_joined, mode="w", encoding="utf-8") as joined_file:
        for line in results:
            joined_file.write(line)


if __name__ == "__main__":
    join_aol(PATH_AOL, PATH_JOINED)
