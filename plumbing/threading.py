"""General objects for creating threads with specified classes."""
from __future__ import absolute_import
import threading, time, collections, functools, sys
from biz.handler.queue import Queue
from biz.handler.worker import Worker

class Thread(threading.Thread):
    def __init__(self, app, classname, args, kwargs, kwargs_meta):
        "Custom thread object for the pubsub-framework with handling of queue-events."
        threading.Thread.__init__(self)
        self._app = app
        self._classname = classname
        module, klass = '.'.join(classname.split('.')[:-1]), classname.split('.')[-1]
        try:
            __import__(module, fromlist=[klass])
        except ImportError:
            raise ImportError('No module named %s' % module_name)
        try:
            mod = sys.modules[module]
        except KeyError:
            raise ImportError('Module "{}" not found after successful import.'.format(module))
        try:
            self._worker = mod.__dict__[klass](*args, **kwargs)
            assert isinstance(self._worker, Worker)
            self._queue = Queue(app, self._worker, **kwargs_meta)
            self._worker.queue = self._queue
        except Exception as e:
            raise e.__class__('{} for \n{}({}{}{})'.format(str(e), 
                                                           '{}.{}'.format(module, klass),
                                                           ', '.join([repr(arg) for arg in args]), 
                                                           ', ' if kwargs else '',
                                                           ', '.join(['{}={}'.format(k,repr(v)) for k,v in kwargs.items()])))
    def run(self):
        print self._worker, self._queue
        self._queue.start()  # Enables receiving messages
        self._worker.start() # If it is a thread, start processing. If it is a simple worker, ignore.

class Factory(object):
    """Create new threads based on the supplied specification of the class name."""
    def __init__(self, app):
        self._app = app
        self._threads = []
    def create(self, classname, args, kwargs, kwargs_meta):
        thread = Thread(self._app, classname, args, kwargs, kwargs_meta)
        self._threads.append(thread)
    def start(self):
        for thread in self._threads:
            thread.start()
