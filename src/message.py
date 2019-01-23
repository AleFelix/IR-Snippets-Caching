# -*- coding: utf-8 -*-


class Message(object):
    def __init__(self, func, parameters, task):
        self.function = func
        self.parameters = parameters
        self.task = task
        self.result = None

    def execute_function(self):
        self.result = self.function(*self.parameters)
