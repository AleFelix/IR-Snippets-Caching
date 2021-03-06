# -*- coding: utf-8 -*-

from nltk import word_tokenize, sent_tokenize
from collections import Counter

LIMIT = 0.3
MAX_DISTANCE = 4
MIN_SIZE = 3


def get_terms_text(text, stop_words):
    return [token.lower() for token in word_tokenize(text) if token not in stop_words and len(token) >= MIN_SIZE]


def get_sentences(text_doc):
    # text_doc = text_doc.replace("\n", " ")
    all_sentences = []
    for line in text_doc.splitlines():
        sentences = sent_tokenize(line)
        all_sentences.extend(sentences)
    return all_sentences


def get_relevant_terms(text_doc, stop_words):
    stop_words = set(stop_words)
    terms = get_terms_text(text_doc, stop_words)
    terms_dist = Counter(terms)
    terms_limit = int(len(terms) * LIMIT)
    freq_count = 0
    relevant_terms = set()
    for term, freq in terms_dist.most_common():
        relevant_terms.add(term)
        freq_count += freq
        if freq_count >= terms_limit:
            break
    return relevant_terms


def compute_sentence_relevance(sentence, relevant_terms, stop_words):
    sentence = get_terms_text(sentence, stop_words)
    segments_relevance = []
    number_relevants_segment = 0
    number_terms_found = 0
    number_terms_segment = 0
    start_position = None
    end_position = None
    close_segment = False
    for position, term in enumerate(sentence):
        if start_position is None:
            if term in relevant_terms:
                start_position = position
                end_position = position
                number_relevants_segment += 1
                number_terms_found += 1
                number_terms_segment = number_terms_found
        else:
            if position - end_position <= MAX_DISTANCE:
                if term in relevant_terms:
                    end_position = position
                    number_relevants_segment += 1
                    number_terms_found += 1
                    number_terms_segment = number_terms_found
                else:
                    number_terms_found += 1
            else:
                close_segment = True
        if position == len(sentence) - 1 and number_terms_segment > 0:
            close_segment = True
        if close_segment:
            relevance = (number_relevants_segment ** 2) / float(number_terms_segment)
            segments_relevance.append(relevance)
            start_position = None
            end_position = None
            close_segment = False
            number_relevants_segment = 0
            number_terms_found = 0
            number_terms_segment = 0
            if position < len(sentence) - 1 and term in relevant_terms:
                start_position = position
                end_position = position
                number_relevants_segment += 1
                number_terms_found += 1
                number_terms_segment = number_terms_found
    return max(segments_relevance) if segments_relevance else 0


def compute_sentence_query_relevance(sentence, query):
    sentence = [token.lower() for token in word_tokenize(sentence) if len(token) >= MIN_SIZE]
    query_count = sum(1 for token in sentence if token in set(query))
    return (query_count ** 2) / float(len(query)) if len(query) > 0 else 0


def summarize_document(text_doc, stop_words, max_size=None, query=None, max_sent=None, w_query=None, w_sent=None):
    text_doc = " ".join(text_doc) if type(text_doc) is list else text_doc
    all_sentences = get_sentences(text_doc)
    if query is None:
        max_sent = int(len(all_sentences) * float(max_size))
    relevant_terms = get_relevant_terms(text_doc, stop_words)
    all_relevances = []
    for sentence in all_sentences:
        sent_relevance = compute_sentence_relevance(sentence, relevant_terms, stop_words)
        if query is not None:
            sent_query_relevance = compute_sentence_query_relevance(sentence, query)
            sent_relevance = w_query * sent_query_relevance + w_sent * sent_relevance
        all_relevances.append(sent_relevance)
    sorted_positions = sorted(range(len(all_relevances)), key=lambda i: all_relevances[i], reverse=True)
    if len(sorted_positions) > max_sent:
        sorted_positions = sorted_positions[:max_sent]
    top_sentences = []
    for position in sorted(sorted_positions):
        top_sentences.append(all_sentences[position])
    return top_sentences


def generate_summary(text_doc, stop_words, max_size):
    return summarize_document(text_doc, stop_words, max_size)


def generate_snippet(text_doc, stop_words, max_sentences, query, query_weight=0.6, sent_weight=0.4):
    return summarize_document(text_doc, stop_words, None, query, max_sentences, query_weight, sent_weight)


def update_supersnippet(supersnippet, snippet, max_sentences, threshold, stop_words):
    sets_supersnippet = []
    if supersnippet is not None:
        for sentence_ss in supersnippet:
            terms_sent_ss = set(get_terms_text(sentence_ss, stop_words))
            sets_supersnippet.append(terms_sent_ss)
    new_supersnippet = [] if supersnippet is None else list(supersnippet)
    for sentence in snippet:
        terms_sent = set(get_terms_text(sentence, stop_words))
        best_match = {"index": None, "sim": 0}
        for index, terms_sent_ss in enumerate(sets_supersnippet):
            same_terms = terms_sent & terms_sent_ss
            sim = len(same_terms) / float(len(terms_sent)) if len(terms_sent) > 0 else 0
            if best_match["index"] is None or best_match["sim"] < sim:
                best_match["index"] = index
                best_match["sim"] = sim
        if best_match["sim"] <= threshold:
            new_supersnippet.append(sentence)
            sets_supersnippet.append(terms_sent)
        elif best_match["index"] is not None and best_match["index"] != len(new_supersnippet) - 1:
            new_supersnippet.append(new_supersnippet.pop(best_match["index"]))
            sets_supersnippet.append(sets_supersnippet.pop(best_match["index"]))
        if len(new_supersnippet) > int(max_sentences):
            new_supersnippet.pop(0)
            sets_supersnippet.pop(0)
    return new_supersnippet


def has_good_quality(text, query, stop_words):
    terms_query = set(query)
    terms_text = set()
    if type(text) is list:
        for sentence in text:
            terms_sent = set(get_terms_text(sentence, stop_words))
            terms_text = terms_text.union(terms_sent)
    else:
        terms_text = set(get_terms_text(text, stop_words))
    same_terms = terms_query & terms_text
    print "SAME TERMS"
    print same_terms
    print "TERMS QUERY"
    print terms_query
    print "DIVISION"
    print str((len(same_terms) ** 2) / float(len(terms_query)))
    return (len(same_terms) ** 2) / float(len(terms_query)) >= 1 if len(terms_query) > 0 else 0
