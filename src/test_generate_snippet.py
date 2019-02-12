# -*- coding: utf-8 -*-

from file_loader import FileLoader
from document_parser import get_tokens_doc_from_file_data_seek
import cPickle
import codecs
from document_summarizer import generate_snippet

PATH_STOPWORDS = "/home/ale/Repositorios/IR-Snippet-Caching/stopwords/stopword-list.txt"
PATH_FILE = "/home/ale/Repositorios/IR-Snippet-Caching/processed/ClueWeb12_00/0001wb/0001wb-00.txt"
PATH_INDEX = "/home/ale/Repositorios/IR-Snippet-Caching/processed/index"
ID_DOC = u"clueweb12-0001wb-00-00979"


def load_index(path_index):
    with open(path_index, mode="rb") as file_index:
        return cPickle.load(file_index)


def load_stopwords(path_stopwords):
    stopwords = []
    with codecs.open(path_stopwords, mode="r", encoding="utf-8") as file_stopwords:
        for line in file_stopwords:
            stopwords.append(line.strip())
    return stopwords


if __name__ == "__main__":
    stop_words = load_stopwords(PATH_STOPWORDS)
    docs_index = load_index(PATH_INDEX)
    file_loader = FileLoader(200)
    file_data = file_loader.get_file(PATH_FILE)
    index_doc = docs_index[ID_DOC]
    tokens = get_tokens_doc_from_file_data_seek(file_data, index_doc)
    print "TOKENS"
    print tokens
    print "****"
    snippet = generate_snippet(tokens, stop_words, 3, ["turn", "sticky", "poison"])
    for sentence in snippet:
        print sentence
        print "---"
