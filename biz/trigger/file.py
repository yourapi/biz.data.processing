"""Watch file changes on specified path."""
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Thread
from time import sleep, time
from os.path import join, split, splitext, exists
from os import lstat
from datetime import datetime
from biz.handler.worker import Worker

class new(Worker, Thread):
    def __init__(self, path, wait_time=10, recursive=True):
        Thread.__init__(self)
        Worker.__init__(self, path=path, wait_time=wait_time, recursive=recursive)
    def run(self):
        if not exists(self._path):
            raise ValueError('Path "{}" does not exist. watchdog.observers.Observer not initialized.'.format(self._path))
        event_handler = DropEvent(self._wait_time, self.signal)
        observer = Observer()
        observer.schedule(event_handler, self._path, recursive=self._recursive)
        observer.start()
        while True:	
            event_handler.poll()
            sleep(1)
        observer.stop()
        observer.join()
    def signal(self, filename):
        self.send(filename=filename)

AVAILABLE = 3
OPEN = 2
REMOVED = 1
def file_status(filename):
    """Return the status of the file. If not exists, return REMOVED. If it can be opened for reading,return AVAILABLE.
    If it cannot be opened,it is opened by another process, return OPEN."""
    if not exists(filename):
        return REMOVED
    try:
        f = open(filename, 'a+b')
        f.close()
        return AVAILABLE
    except:
        return OPEN

class DropEvent(FileSystemEventHandler):
    "The handler for modified files in the watched directory. TODO: check changed files in meantime (larger, changed timestamp etc)"
    def __init__(self, wait_time, callback=None):
        super(DropEvent, self).__init__()
        # Administrate the events and filenames. Fire event if full new file is ready.
        self._events = {}
        self._wait_time = wait_time
        self._callback = callback if callback else lambda filename: None
    def poll(self):
        "See if any files present which aren't changed in the last x seconds."
        now = time()
        for filename, time_stat in self._events.copy().items():
            time_last, stat_last = time_stat
            try:
                stat = lstat(filename)
            except:
                stat = stat_last
            if now - time_last > self._wait_time and stat == stat_last:
                status = file_status(filename)
                if status in (AVAILABLE, REMOVED):
                    del self._events[filename]
                if status == AVAILABLE:
                    self._callback(filename)
    def on_modified(self, event):
        "The modified-event is always called, even after move/rename or create."
        if event.is_directory:
            return
        filename = event.src_path
        if file_status(filename) in (OPEN, AVAILABLE):
            # File is available, see if it still is after timeout:
            self._events[filename] = (time(), lstat(filename))

if __name__ == '__main__':
    watch()