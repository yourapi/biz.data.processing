"""General objects for creating threads with specified classes."""
import time, collections, functools, sys
from threading import Thread
from indroid.handler.objects import Consumer, Producer
import inspect, hashlib, logging

def receive(worker):
    """Proxy for receiving method, the args and kwargs are supplied as appropriate.
    Named keywords are supplied as arguments if their names match, superfluous arguments
    and keywords are removed, with an entry to the log."""
    func = worker.receive
    args_worker = worker.args or [] if hasattr(worker, 'args') else []
    kwargs_worker = worker.kwargs or {} if hasattr(worker, 'kwargs') else {}
    def proxy(*args, **kwargs):
        argspec = inspect.getargspec(func)
        logging.getLogger().debug('{} {} {} {}'.format(func, argspec, args, kwargs))
        # Fill missing arguments from named arguments. First argument is 'self', 
        # find arguments which aren't specified:
        args = list(args_worker) + list(args)
        kwargs.update(kwargs_worker)
        for arg_name in argspec.args[1 + len(args):]:
            if arg_name in kwargs:
                args.append(kwargs[arg_name])
                del kwargs[arg_name]
            else:
                # No more keywords,leave
                break
        args = tuple(args)
        if not argspec.keywords:
            # Only supply default arguments, when specified. Remove all kwarg-keys
            # which are not present in default argument names.
            args_left = len(argspec.args) - len(args) - 1
            if args_left > 0 and argspec.defaults:
                args_left = min(args_left, len(argspec.defaults))
                keys = set(argspec.args[-args_left:])
                for key in kwargs.copy():
                    if key not in keys:
                        del kwargs[key]
            else:
                kwargs.clear()
        default_len = len(argspec.defaults) if argspec.defaults else 0
        args_left = len(argspec.args) - len(args) - default_len - 1
        if args_left:
            logging.getLogger().error('{} arguments missing for "{}": ({})'.\
                                      format(args_left, func, 
                                             ', '.join(argspec.args[-args_left:-default_len or None])))
            return lambda *args, **kwargs: None
        return func(*args, **kwargs)
    return proxy

class Queue(Thread):
    "Abstraction for the definition of queues, exchanges and their corresponding routing keys."
    def __init__(self, app, worker,
                 exchange=None, exchange_in=None, exchange_out=None, 
                 key=None, key_in=None, key_out=None, queue=None, 
                 exchange_type='direct', exchange_type_in=None, exchange_type_out=None):
        Thread.__init__(self)
        self.app = app
        self.worker = worker
        self._exchange_in = exchange_in or exchange
        self._exchange_out = exchange_out or exchange
        self._key_in = (key_in or key or []) 
        if not isinstance(self._key_in, list):
            self._key_in = [self._key_in]
        self._key_out = (key_out or key or [])
        if not isinstance(self._key_out, list):
            self._key_out = [self._key_out]
        self._exchange_type_in = exchange_type_in or exchange_type
        self._exchange_type_out = exchange_type_out or exchange_type
        if queue is None:
            queue = self._key_in or self._key_out
            if queue:
                queue = queue[0]
            if not queue:
                queue = '{}.{}'.format(self._exchange_type_in, self._exchange_in)
            queue = '.'.join(['q', str(self.index()), queue])  # Make sure the queue name is unique and has as many identifying data as possible
        self._queue = queue
        self._consumers = []
        url = app.config.messaging.url
        for key in self._key_in:
            consumer = Consumer(url, self._exchange_in, key, self._queue, self._exchange_type_in)
            consumer.on_message = receive(worker)  # Connect the worker to the receiving consumer
            self._consumers.append(consumer)
        self._producers = []
        for key in self._key_out:
            producer = Producer(url, self._exchange_out, key, self._exchange_type_out)
            self._producers.append(producer)
    def index(self):
        "Return a unique but repeatable index based on the exchanges and the keys, both in and out."
        return hashlib.md5(''.join([self._exchange_in or '', self._exchange_out or ''] + self._key_in + self._key_out)).hexdigest()
    def run(self):
        for consumer in self._consumers:
            consumer.run()
    def send(self, *args, **kwargs):
        for producer in self._producers:
            producer.send(*args, **kwargs)
    def __repr__(self):
        return '<queue {}, {}, {}, {}>'.format(self._exchange_in, self._exchange_out, self._key_in,self._key_out)