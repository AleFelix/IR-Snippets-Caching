# -*- coding: utf-8 -*-

import codecs
from document_parser import get_html_doc, clean_html, get_document_path
from document_summarizer import generate_snippet

ROOT_CORPUS = "clueweb12/ClueWeb12_"
MAX_SENTENCES = 3
PATH_STOPWORDS = "stopwords/stopword-list.txt"


def load_stopwords(path_stopwords):
    stopwords = []
    with codecs.open(path_stopwords, mode="r", encoding="utf-8") as file_stopwords:
        for line in file_stopwords:
            stopwords.append(line.strip())
    return stopwords


def load_results(path_results):
    dict_results = {}
    with codecs.open(path_results, mode="r", encoding="utf-8") as file_results:
        for line_result in file_results:
            items_result = line_result.strip().split()
            id_query = items_result[1]
            if id_query not in dict_results:
                dict_results[id_query] = {"ids_docs": [], "paths_docs": []}
            id_doc = items_result[2]
            path_doc = get_document_path(ROOT_CORPUS, id_doc)
            dict_results[id_query]["ids_docs"].append(id_doc)
            dict_results[id_query]["paths_docs"].append(path_doc)
    return dict_results


def main():
    stopwords = load_stopwords(PATH_STOPWORDS)
    dic_r = load_results("result_list/InL2c1.0_0.res")
    for query in dic_r:
        print query
        print dic_r[query]["ids_docs"]
        print dic_r[query]["paths_docs"]
        for index, id_doc in enumerate(dic_r[query]["ids_docs"]):
            path_doc = dic_r[query]["paths_docs"][index]
            html = get_html_doc(id_doc, path_doc)
            text = clean_html(html)
            if index == 1:
                print
                print id_doc
                print
                print text
                summary = generate_snippet(text, stopwords, MAX_SENTENCES, ["articles"])
                print
                print summary


if __name__ == "__main__":
    main()
