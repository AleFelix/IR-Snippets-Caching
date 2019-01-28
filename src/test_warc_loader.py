# -*- coding: utf-8 -*-

import gzip
import re

ID_DOC = "clueweb12-0000wb-00-00027"
PATH_DOC = "/home/ale/Repositorios/IR-Snippet-Caching/clueweb12/ClueWeb12_00/0000wb/0000wb-00.warc.gz"


def get_doc(id_doc, path_doc):
    with gzip.open(path_doc, mode="rb") as file_stream:
        file_data = file_stream.read()
    # regex = re.compile("WARC-Type: response(?:.|\r\n|\r|\n(?!WARC-Type: response))*WARC-TREC-ID: " + str(id_doc) +
    #                    "(?:.|\r\n|\r|\n)*?Content-Length:.*(?:\r\n|\r|\n)(?:\r\n|\r|\n)((?:.|\r\n|\r|\n)*?)" +
    #                    "(?:WARC/[0-9]\.[0-9](?:\r\n|\r|\n)WARC-Type:|$)")
    # print regex.search(file_data).group(1)
    found_doc = False
    loading_doc = False
    # content_length = 0
    document = ""
    for line in file_data.splitlines():
        if not found_doc and line == "WARC-TREC-ID: " + str(id_doc):
            found_doc = True
        elif found_doc and not loading_doc and "Content-Length: " in line:
            # content_length = int(line.replace("Content-Length: ", ""))
            loading_doc = True
        elif found_doc and loading_doc and line != "WARC/1.0":
            document += line + "\n"
            # content_length -= len(line + "\n")
        elif found_doc and loading_doc and line == "WARC/1.0":
            break
    if found_doc:
        print document


if __name__ == "__main__":
    get_doc(ID_DOC, PATH_DOC)
