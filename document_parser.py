# -*- coding: utf-8 -*-

import warc
from bs4 import BeautifulSoup


def get_html_doc(id_doc, path_doc):
    html_doc = None
    file_doc = warc.open(path_doc)
    for record in file_doc:
        if "WARC-TREC-ID" in record and record["WARC-TREC-ID"] == id_doc:
            html_doc = record.payload.read()
            break
    file_doc.close()
    return html_doc


def clean_html(html_doc):
    soup = BeautifulSoup(html_doc, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text_doc = soup.get_text(separator=u" ")
    lines = (line.strip() for line in text_doc.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("   "))
    text_doc = '\n'.join(chunk for chunk in chunks if chunk)
    return text_doc
