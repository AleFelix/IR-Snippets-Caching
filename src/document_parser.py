# -*- coding: utf-8 -*-


import StringIO


def get_document_path(root_corpus, id_doc):
    items_id_doc = id_doc.split("-")
    section = items_id_doc[1]
    section_num = section[:2]
    filename = section + "-" + items_id_doc[2] + ".txt"
    path_doc = root_corpus + section_num + "/" + section + "/" + filename
    return path_doc


def get_tokens_doc_from_file_data_seek(binary_file_data, index_doc):
    virtual_file = StringIO.StringIO(binary_file_data)
    virtual_file.seek(index_doc["start"])
    text_tokens = virtual_file.read(index_doc["length"])
    virtual_file.close()
    return unicode(text_tokens, errors="ignore")


def get_tokens_doc_with_seek(path_file, index_doc):
    with open(path_file, mode="rb") as file_stream:
        file_stream.seek(index_doc["start"])
        text_tokens = file_stream.read(index_doc["length"])
        return unicode(text_tokens, errors="ignore")
