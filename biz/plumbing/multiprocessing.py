"""Multiprocessing-container for """
import time, collections, functools
import multiprocessing

class Process(multiprocessing.Process):
    def __init__(self, classname):
        multiprocessing.Process.__init__(self)
        self._classname = classname
        module, klass = '.'.join(classname.split('.')[:-1]), classname.split('.')[-1]
        exec('from {} import {}'.format(module, klass))
        self._object = eval('{}()'.format(klass))
    def run(self):
        print self._object
        self._object()

class Factory(object):
    """Create new processes based on the supplied specification of the class name."""
    def __init__(self, app):
        self._app = app
        self._processes = []
    def create(self, classname):
        process = Process(classname)
        self._processes.append(process)
    def start(self):
        for process in self._processes:
            process.start()
        #for process in self._processes:
            #process.join()
