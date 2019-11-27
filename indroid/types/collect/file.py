"""File objects for handling all kinds of file-events and actions."""
import os, re, shutil, datetime
from os.path import join, split, splitext, exists
from indroid.handler.worker import Worker

class dispatch(Worker):
    def receive(self, filename, pattern, flags=re.I):
        path, name = split(filename)
        if re.match(pattern, name, flags):
            self.send(filename=filename, *self.args, **self.kwargs)

class copy_move(Worker):
    def receive(self, filename, destination):
        path, name = split(filename)
        if '{date}' in destination:
            r = re.match('.*?(\d+\-\d+\-\d+).*', name)
            date = r.group(1) if r else str(datetime.date.today())
            destination = destination.format(date=date)
        if not exists(destination):
            os.makedirs(destination)
        try:
            self.file_operate(filename, destination)
        except Exception as e:
            # ToDo: make log
            print e
    def file_operate(self, filename, destination):
        raise NotImplementedError
    
class copy(copy_move):
    def file_operate(self, filename, destination):
        shutil.copy2(filename, destination)

class move(copy_move):
    def file_operate(self, filename, destination):
        shutil.move(filename, destination)

