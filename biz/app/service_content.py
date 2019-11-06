### Run Python scripts as a service example (ryrobes.com)
### Usage : python aservice.py install (or / then start, stop, remove)
from time import sleep
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import datetime

class BizEvent(FileSystemEventHandler):
    def on_any_event(self, event):
        with open(r"P:\download\test.txt", 'a') as f:
            f.write(str(datetime.datetime.now()))
            f.write(str(event) + '\n')
            #f.write(event.event_type + '\n')
            #f.write(event.src_path + '\n')
            #f.write(str(event.is_directory) + '\n')

def watch():
    event_handler = BizEvent()
    observer = Observer()
    observer.schedule(event_handler, r"P:\data\download", recursive=True)
    observer.start()
    while True:	
        sleep(1)
    observer.stop()
    observer.join()

watch()