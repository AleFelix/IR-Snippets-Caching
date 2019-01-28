# -*- coding: utf-8 -*-

import warc
# import io
# import subprocess
# from warcio.archiveiterator import ArchiveIterator
# from bs4 import BeautifulSoup
import bs4
import mmap
import gzip
import timer
import io
import warcio.archiveiterator


def get_document_path(root_corpus, id_doc):
    items_id_doc = id_doc.split("-")
    section = items_id_doc[1]
    section_num = section[:2]
    filename = section + "-" + items_id_doc[2] + ".warc.gz"
    path_doc = root_corpus + section_num + "/" + section + "/" + filename
    return path_doc


def get_html_doc(id_doc, path_doc):
    html_doc = None
    # # timer = Timer()
    # # timer.restart()
    # # gzip = subprocess.Popen(['/home/ale/Portables/pigz/pigz-2.4/unpigz', '-cdfq', path_doc], stdout=subprocess.PIPE)
    # # timer.stop()
    # # print "GZIP: " + str(timer.total_time)
    # # timer.restart()
    # # the_file = io.BytesIO(gzip.stdout.read())
    # # timer.stop()
    # # print "the_file: " + str(timer.total_time)
    # # the_file.name = 'file.warc'
    ti = timer.Timer()
    file_stream = open(path_doc, mode="rb")
    # # ti.restart()
    # # mm = mmap.mmap(file_stream.fileno(), 0, access=mmap.ACCESS_READ)
    # # ti.stop()
    # # print "TIEMPO DE MAPEO: " + str(ti.total_time)
    ti.restart()
    # # virtual_file = io.BytesIO(mm.read(mm.size()))
    virtual_file = io.BytesIO(file_stream.read())
    ti.stop()
    # # mm.close()
    print "TIEMPO DE LECTURA: " + str(ti.total_time)
    file_stream.close()
    # ti.restart()
    # decompressed_file = gzip.GzipFile(fileobj=virtual_file, mode='rb')
    # ti.stop()
    # print "TIEMPO DE DESCOMPRESION: " + str(ti.total_time)
    # ti.restart()
    # file_doc = warc.WARCFile(fileobj=decompressed_file)
    # for record in file_doc:
    #     if "WARC-TREC-ID" in record and record["WARC-TREC-ID"] == id_doc:
    #         html_doc = record.payload.read()
    #         break
    # ti.stop()
    # print "TIEMPO DE RECUPERACION: " + str(ti.total_time)
    # file_doc.close()
    # virtual_file.close()
    # gz = gzip.open(path_doc, 'rb')
    gz = gzip.GzipFile(fileobj=virtual_file, mode='rb')
    # f = io.BufferedReader(gz)
    ti.restart()
    # with gzip.open(path_doc, 'rb') as stream:
    for record in warcio.archiveiterator.ArchiveIterator(gz):
        if record.rec_type == 'response' and record.rec_headers.get_header('WARC-TREC-ID') == id_doc:
            html_doc = record.raw_stream.read()
            break
    ti.stop()
    print "TIEMPO DE RECUPERACION: " + str(ti.total_time)
    # f.close()
    virtual_file.close()
    gz.close()
    return html_doc


def get_html_doc_from_file_data(id_doc, file_data):
    html_doc = None
    virtual_file = io.BytesIO(file_data)
    # gz = gzip.GzipFile(fileobj=virtual_file, mode='rb')
    ti = timer.Timer()
    ti.restart()
    for record in warcio.archiveiterator.ArchiveIterator(virtual_file):
        if record.rec_type == 'response' and record.rec_headers.get_header('WARC-TREC-ID') == id_doc:
            html_doc = record.raw_stream.read()
            break
    ti.stop()
    print "TIEMPO DE RECUPERACION: " + str(ti.total_time)
    # gz.close()
    virtual_file.close()
    return html_doc


def get_html_doc_from_file_data_fast(id_doc, file_data):
    ti = timer.Timer()
    ti.restart()
    found_doc = False
    loading_doc = False
    html_doc = ""
    for line in file_data.splitlines():
        if not found_doc and line == "WARC-TREC-ID: " + str(id_doc):
            found_doc = True
        elif found_doc and not loading_doc and "Content-Length: " in line:
            loading_doc = True
        elif found_doc and loading_doc and line != "WARC/1.0":
            html_doc += line + "\n"
        elif found_doc and loading_doc and line == "WARC/1.0":
            break
    ti.stop()
    print "TIEMPO DE RECUPERACION FAST: " + str(ti.total_time)
    if found_doc:
        return html_doc


def get_html_doc_from_file_data_seek(id_doc, file_data, index):
    if id_doc not in index:
        return
    with io.BytesIO(file_data) as virtual_file:
        virtual_file.seek(index[id_doc]["start"])
        html = []
        for line in virtual_file:
            if "WARC/1.0" in line:
                break
            html.append(line)
        return "".join(html)
        # return virtual_file.read(index[id_doc]["end"])


def get_html_doc_with_seek(id_doc, path_file, index):
    with gzip.open(path_file, mode="rb") as file_stream:
        file_stream.seek(index[id_doc]["start"])
        html = ""
        for line in file_stream:
            if "WARC/1.0" in line:
                break
            html += line
        return html
        # return file_stream.read(index[id_doc]["end"])


def clean_html(html_doc):
    soup = bs4.BeautifulSoup(html_doc, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text_doc = soup.get_text(separator=u" ")
    lines = (line.strip() for line in text_doc.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("   "))
    text_doc = '\n'.join(chunk for chunk in chunks if chunk)
    return text_doc
