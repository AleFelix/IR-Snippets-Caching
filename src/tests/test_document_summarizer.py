# -*- coding: utf-8 -*-

from src.document_summarizer import compute_sentence_relevance


def test_compute_sentence_relevance():
    assert compute_sentence_relevance("", set()) == 0
    assert compute_sentence_relevance("term term term term term", set()) == 0
    assert compute_sentence_relevance("term1 term2 term3 term4 term5", set()) == 0
    assert compute_sentence_relevance("", {"rel1", "rel2", "rel3"}) == 0
    assert compute_sentence_relevance("term", {"rel1", "rel2", "rel3"}) == 0
    assert compute_sentence_relevance("rel1", {"rel1", "rel2", "rel3"}) == (1 ** 2) / float(1)
    assert compute_sentence_relevance("rel1 rel2", {"rel1", "rel2", "rel3"}) == (2 ** 2) / float(2)
    assert compute_sentence_relevance("rel1 term rel2", {"rel1", "rel2", "rel3"}) == (2 ** 2) / float(3)
    assert compute_sentence_relevance("rel1 term rel2 term", {"rel1", "rel2", "rel3"}) == (2 ** 2) / float(3)
    assert compute_sentence_relevance("term rel1 term rel2", {"rel1", "rel2", "rel3"}) == (2 ** 2) / float(3)
    assert compute_sentence_relevance("rel1 term rel2 rel2", {"rel1", "rel2", "rel3"}) == (3 ** 2) / float(4)
    assert compute_sentence_relevance("rel1 term1", {"rel1", "rel2", "rel3"}) == (1 ** 2) / float(1)
    assert compute_sentence_relevance("relev term term relev term", {"relev"}) == (2 ** 2) / float(4)
    assert compute_sentence_relevance("relev relev term relev term", {"relev"}) == (3 ** 2) / float(4)
    assert compute_sentence_relevance("relev term term term term relev", {"relev"}) == (1 ** 2) / float(1)
    assert compute_sentence_relevance("relev term term term relev term", {"relev"}) == (2 ** 2) / float(5)
    assert compute_sentence_relevance("term term term term term", {"relev"}) == 0
