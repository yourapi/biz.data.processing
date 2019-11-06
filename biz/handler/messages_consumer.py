#!/usr/bin/env python
import sys, pika, yaml
from biz.handler.objects import Consumer

class C1(Consumer):
    def on_message(self, filename):
        print filename

def main():
    example = C1('amqp://guest:guest@localhost:5672/%2F', *sys.argv[1:])
    try:
        example.run()
    except KeyboardInterrupt:
        example.stop()

if __name__ == '__main__':
    main()