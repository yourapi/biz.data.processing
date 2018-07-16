#!/usr/bin/env python
"""Message-classes for simple sending of messages to an exchange."""
import pika, yaml
from threading import Thread
from time import sleep

class Worker(Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        self._queue = None
        self.args = args
        self.kwargs = kwargs
        for k, v in kwargs.items():
            setattr(self, '_' + k, v)
    @property
    def queue(self):
        return self._queue
    @queue.setter
    def queue(self, value):
        self._queue = value
    def receive(self, *args, **kwargs):
        "This method must be overridden, the worker-method for this class."
        raise NotImplementedError
    def run(self):
        "Do nothing to emulate starting thread, leave immediately."
        pass
    def send(self, *args, **kwargs):
        """Send the specified arguments and keyword-arguments. The arguments are completed with the 
        stored arguments from the worker instance."""
        args = tuple(list(args) + list(self.args))
        kwargs_new = self.kwargs.copy()
        kwargs_new.update(kwargs)
        self.queue.send(*args, **kwargs_new)