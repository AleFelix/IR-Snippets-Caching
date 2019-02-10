# -*- coding: utf-8 -*-

import os
import ConfigParser
from parallel_analyzer import SnippetAnalyzer

CONFIGURATION = "configuration"
FILE_PATH = os.path.abspath(os.path.dirname(__file__))


def get_config_options(config_parser, options):
    configuration = {}
    for option_name in options:
        configuration[option_name] = config_parser.get(CONFIGURATION, option_name)
    return configuration


def master_main():
    options = ["path_results", "path_queries", "path_stopwords", "path_index", "root_corpus", "snippet_size",
               "max_queries", "surrogate_size", "ssnippet_sizes", "ssnippet_threshold", "cache_memory_sizes",
               "dir_output"]
    config_parser = ConfigParser.ConfigParser()
    # try:
    config_parser.readfp(open(FILE_PATH + "/analyzer.conf"))
    configuration = get_config_options(config_parser, options)
    cache_memory_sizes = [int(size) for size in configuration["cache_memory_sizes"].split(",")]
    ssnippet_sizes = [int(size) for size in configuration["ssnippet_sizes"].split(",")]
    snippet_analyzer = SnippetAnalyzer(configuration["path_results"], configuration["path_queries"],
                                       configuration["path_stopwords"], configuration["path_index"],
                                       configuration["root_corpus"], configuration["snippet_size"],
                                       configuration["max_queries"], configuration["surrogate_size"], ssnippet_sizes,
                                       configuration["ssnippet_threshold"], cache_memory_sizes,
                                       configuration["dir_output"])
    snippet_analyzer.start_analysis()
    print snippet_analyzer.statistics
    # except IOError, exception:
    #    print "ERROR: " + str(exception)
    # except ConfigParser.NoSectionError:
    #   print "ERROR: Section [" + CONFIGURATION + "] not found in analyzer.conf"
    # except ConfigParser.NoOptionError, exception:
    #    print "ERROR: " + exception.message


if __name__ == '__main__':
    master_main()
