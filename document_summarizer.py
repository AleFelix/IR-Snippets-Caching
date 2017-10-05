# -*- coding: utf-8 -*-

from nltk import word_tokenize, sent_tokenize, FreqDist

LIMIT = 0.3
MAX_DISTANCE = 4
MIN_SIZE = 3


def get_sentences(text_doc):
    all_sentences = []
    for line in text_doc.splitlines():
        sentences = sent_tokenize(line)
        all_sentences.extend(sentences)
    return all_sentences


def get_relevant_terms(text_doc, stop_words):
    tokens = [token.lower() for token in word_tokenize(text_doc)]
    stop_words = set(stop_words)
    terms = [token for token in tokens if token not in stop_words and len(token) >= MIN_SIZE]
    terms_dist = FreqDist(terms)
    terms_limit = terms_dist.N() * LIMIT
    freq_count = 0
    relevant_terms = set()
    for term, freq in terms_dist.most_common():
        relevant_terms.add(term)
        freq_count += freq
        if freq_count >= terms_limit:
            break
    return relevant_terms


def compute_sentence_relevance(sentence, relevant_terms):
    sentence = [token.lower() for token in word_tokenize(sentence) if len(token) >= MIN_SIZE]
    segments_relevance = []
    number_relevants = 0
    number_terms = 0
    number_total_terms = 0
    start_position = None
    temp_end_position = None
    end_position = None
    for position, term in enumerate(sentence):
        if start_position is None:
            if term in relevant_terms:
                start_position = position
                temp_end_position = position
                number_relevants += 1
                number_terms += 1
                number_total_terms = number_terms
        else:
            if position - temp_end_position < MAX_DISTANCE:
                if term in relevant_terms:
                    temp_end_position = position
                    number_relevants += 1
                    number_terms += 1
                    number_total_terms = number_terms
                else:
                    number_terms += 1
                if position == len(sentence) - 1:
                    end_position = temp_end_position
            else:
                end_position = temp_end_position
        if end_position is not None:
            relevance = (number_relevants ** 2) / float(number_total_terms)
            segments_relevance.append(relevance)
            start_position = None
            temp_end_position = None
            end_position = None
            number_relevants = 0
            number_terms = 0
            number_total_terms = 0
            if position < len(sentence) - 1 and term in relevant_terms:
                start_position = position
                temp_end_position = position
                number_relevants += 1
                number_terms += 1
                number_total_terms = number_terms
    return max(segments_relevance) if segments_relevance else 0


def compute_sentence_query_relevance(sentence, query):
    sentence = [token.lower() for token in word_tokenize(sentence) if len(token) >= MIN_SIZE]
    query_count = sum(1 for token in sentence if token in set(query))
    return (query_count ** 2) / float(len(query))


def summarize_document(text_doc, stop_words, max_size=None, query=None, max_sent=None, w_query=None, w_sent=None):
    all_sentences = get_sentences(text_doc)
    if query is not None:
        max_sent = int(len(all_sentences) * max_size)
    relevant_terms = get_relevant_terms(text_doc, stop_words)
    all_relevances = []
    for sentence in all_sentences:
        sent_relevance = compute_sentence_relevance(sentence, relevant_terms)
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
    query = [token.lower() for token in query]
    return summarize_document(text_doc, stop_words, None, query, max_sentences, query_weight, sent_weight)
