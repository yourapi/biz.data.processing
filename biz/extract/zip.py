"""Extract file with zip-compression"""
from os.path import join, split, splitext, exists
from biz.handler.worker import Worker
import os, shutil
from zipfile import ZipFile

class unzip(Worker):
    def receive(self, filename, destination, passwords):
        path, name = split(filename)
        dest = join(path, destination)
        try:
            z = ZipFile(filename)
            for member in z.filelist:
                name = member.filename
                p = split(name)[0]
                dest2 = join(dest, p)
                if not exists(dest2):
                    os.makedirs(dest2)
                try:
                    z.extract(member, dest2)
                except RuntimeError as err:
                    if 'encrypted' in err.message:
                        for pwd in passwords:
                            try:
                                z.extract(member, dest2, pwd=pwd)
                                break
                            except:
                                pass
            z.close()
        except Exception as e:
            print e