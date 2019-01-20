# -*- coding: utf-8 -*-

import timeit


class Timer(object):
    def __init__(self):
        self.start_time = None
        self.total_time = 0

    def start(self):
        self.start_time = timeit.default_timer()

    def stop(self):
        self.total_time += timeit.default_timer() - self.start_time
        self.start_time = None

    def reset(self):
        self.start_time = None
        self.total_time = 0

    def restart(self):
        self.reset()
        self.start()
