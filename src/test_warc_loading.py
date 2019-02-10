# -*- coding: utf-8 -*-

import io
import warc
import gzip
import time
from clueweb12_parallel_parser import clean_html, get_sentences

PATHS_WARC = ["/home/ale/Repositorios/IR-Snippet-Caching/clueweb12/ClueWeb12_00/0001wb/0001wb-72.warc.gz"]


def load_file(path_file):
    print "Subprocess Started Loading: " + path_file
    with open(path_file, "rb") as compressed_file:
        virtual_file = io.BytesIO(compressed_file.read())
    decompressed_file = gzip.GzipFile(fileobj=virtual_file, mode='rb')
    file_doc = warc.WARCFile(fileobj=decompressed_file)
    documents = []
    for record in file_doc:
        if "WARC-TREC-ID" in record:
            print record["WARC-TREC-ID"]
            id_doc = record["WARC-TREC-ID"]
            document = record.payload.read()
            print "READED"
            document = clean_html(document)
            print "CLEANED"
            document = unicode(document, "utf-8", errors="ignore")
            print "UNICODED"
            document = get_sentences(document)
            print "SENTENCED"
            documents.append({"id-doc": id_doc, "text": document})
    file_doc.close()
    decompressed_file.close()
    virtual_file.close()
    print "Subprocess Finished Loading: " + path_file


if __name__ == "__main__":
    start = time.time()
    for path_warc in PATHS_WARC:
        load_file(path_warc)
    print "TOTAL TIME: " + str(time.time() - start) + "s"
