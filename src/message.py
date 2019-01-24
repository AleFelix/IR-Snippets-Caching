# -*- coding: utf-8 -*-


class Message(object):
    def __init__(self, func, parameters, task, result=None):
        self.function = func
        self.parameters = parameters
        self.task = task
        self.result = result

    def execute_function(self):
        self.result = self.function(*self.parameters)

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)
