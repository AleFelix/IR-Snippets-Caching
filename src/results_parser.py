# -*- coding: utf-8 -*-

import codecs
from document_parser import get_html_doc, clean_html, get_document_path
from document_summarizer import generate_snippet

ROOT_CORPUS = "clueweb12/ClueWeb12_"
MAX_SENTENCES = 3
PATH_STOPWORDS = "stopwords/stopword-list.txt"
PATH_RESULTS = "result_list/PL2_2.res"
PATH_QUERIES = "queries/AOL-1000.txt"


def load_stopwords(path_stopwords):
    stopwords = []
    with codecs.open(path_stopwords, mode="r", encoding="utf-8") as file_stopwords:
        for line in file_stopwords:
            stopwords.append(line.strip())
    return stopwords


def load_results(root_corpus, path_results):
    dict_results = {}
    with codecs.open(path_results, mode="r", encoding="utf-8") as file_results:
        for line_result in file_results:
            items_result = line_result.strip().split()
            id_query = items_result[0]
            if id_query not in dict_results:
                dict_results[id_query] = {"ids_docs": [], "paths_docs": []}
            id_doc = items_result[2]
            path_doc = get_document_path(root_corpus, id_doc)
            dict_results[id_query]["ids_docs"].append(id_doc)
            dict_results[id_query]["paths_docs"].append(path_doc)
    return dict_results


def load_query(path_queries, id_query):
    with codecs.open(path_queries, mode="r", encoding="utf-8") as file_queries:
        for pos_line, line_query in enumerate(file_queries):
            if pos_line == int(id_query) - 1:
                return line_query.strip().split()


def main():
    stopwords = load_stopwords(PATH_STOPWORDS)
    dic_r = load_results(ROOT_CORPUS, PATH_RESULTS)
    for query in dic_r:
        #print query
        #print dic_r[query]["ids_docs"]
        #print dic_r[query]["paths_docs"]
        for index, id_doc in enumerate(dic_r[query]["ids_docs"]):
            path_doc = dic_r[query]["paths_docs"][index]
            html = get_html_doc(id_doc, path_doc)
            text = clean_html(html)
            terms_query = load_query(PATH_QUERIES, query)
            if index != -1:
                print
                print id_doc
                #print
                #print text
                print
                print terms_query
                summary = generate_snippet(text, stopwords, MAX_SENTENCES, terms_query)
                print
                print summary


if __name__ == "__main__":
    main()
