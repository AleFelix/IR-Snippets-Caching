# -*- coding: utf-8 -*-

import os
import re
import codecs

PATH_RESULTS = "/home/ale/Repositorios/IR-Snippet-Caching/split-results"
PATH_JOINED = "/home/ale/Repositorios/IR-Snippet-Caching/split-results/joined.txt"


def join_results(path_results, path_joined):
    results = []
    for file_name in sorted(os.listdir(path_results)):
        if file_name.endswith(".res"):
            file_number = int(re.findall("[0-9][0-9][0-9][0-9][0-9]", file_name)[0])
            path_file = os.path.join(path_results, file_name)
            with codecs.open(path_file, mode="r", encoding="utf-8") as results_file:
                for line in results_file:
                    parts_result = line.split()
                    parts_result[0] = str(int(parts_result[0]) + (file_number * 5000))
                    results.append(" ".join(parts_result))
    with codecs.open(path_joined, mode="w", encoding="utf-8") as joined_file:
        for line in results:
            joined_file.write(line + "\n")


if __name__ == "__main__":
    join_results(PATH_RESULTS, PATH_JOINED)
