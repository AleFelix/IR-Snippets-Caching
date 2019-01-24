# -*- coding: utf-8 -*-

import warc
# import io
# import gzip
# import subprocess
# from warcio.archiveiterator import ArchiveIterator
# from bs4 import BeautifulSoup
import bs4
# from timer import Timer


def get_document_path(root_corpus, id_doc):
    items_id_doc = id_doc.split("-")
    section = items_id_doc[1]
    section_num = section[:2]
    filename = section + "-" + items_id_doc[2] + ".warc.gz"
    path_doc = root_corpus + section_num + "/" + section + "/" + filename
    return path_doc


def get_html_doc(id_doc, path_doc):
    html_doc = None
    # timer = Timer()
    # timer.restart()
    # gzip = subprocess.Popen(['/home/ale/Portables/pigz/pigz-2.4/unpigz', '-cdfq', path_doc], stdout=subprocess.PIPE)
    # timer.stop()
    # print "GZIP: " + str(timer.total_time)
    # timer.restart()
    # the_file = io.BytesIO(gzip.stdout.read())
    # timer.stop()
    # print "the_file: " + str(timer.total_time)
    # the_file.name = 'file.warc'
    file_doc = warc.open(path_doc)
    # file_doc = warc.WARCFile(fileobj=gzip.stdout)
    for record in file_doc:
        if "WARC-TREC-ID" in record and record["WARC-TREC-ID"] == id_doc:
            html_doc = record.payload.read()
            break
    file_doc.close()
    # gz = gzip.open(path_doc, 'rb')
    # f = io.BufferedReader(gz)
    # with gzip.open(path_doc, 'rb') as stream:
    # for record in ArchiveIterator(the_file):
    #     if record.rec_type == 'response' and record.rec_headers.get_header('WARC-TREC-ID') == id_doc:
    #         html_doc = record.raw_stream.read()
    #         break
    # f.close()
    # gzip.terminate()
    return html_doc


def clean_html(html_doc):
    soup = bs4.BeautifulSoup(html_doc, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text_doc = soup.get_text(separator=u" ")
    lines = (line.strip() for line in text_doc.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("   "))
    text_doc = '\n'.join(chunk for chunk in chunks if chunk)
    return text_doc
