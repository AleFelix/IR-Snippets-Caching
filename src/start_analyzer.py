# -*- coding: utf-8 -*-

import os
import ConfigParser
from snippet_analyzer import SnippetAnalyzer

CONFIGURATION = "configuration"
FILE_PATH = os.path.abspath(os.path.dirname(__file__))


def get_config_options(config_parser, options):
    configuration = {}
    for option_name in options:
        configuration[option_name] = config_parser.get(CONFIGURATION, option_name)
    return configuration


def main():
    options = ["path_results", "path_queries", "path_stopwords", "root_corpus", "snippet_size", "max_queries",
               "surrogate_size", "ssnippet_size", "ssnippet_threshold", "cache_memory_size"]
    config_parser = ConfigParser.ConfigParser()
    try:
        config_parser.readfp(open(FILE_PATH + "/analyzer.conf"))
        configuration = get_config_options(config_parser, options)
        snippet_analyzer = SnippetAnalyzer(configuration["path_results"], configuration["path_queries"],
                                           configuration["path_stopwords"], configuration["root_corpus"],
                                           configuration["snippet_size"], configuration["max_queries"],
                                           configuration["surrogate_size"], configuration["ssnippet_size"],
                                           configuration["ssnippet_threshold"], configuration["cache_memory_size"])
        snippet_analyzer.start_analysis()
    except IOError, exception:
        print "ERROR: " + str(exception)
    except ConfigParser.NoSectionError:
        print "ERROR: Section [" + CONFIGURATION + "] not found in analyzer.conf"
    except ConfigParser.NoOptionError, exception:
        print "ERROR: " + exception.message


if __name__ == '__main__':
    main()
