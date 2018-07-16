"""Message-classes."""
import os, subprocess
from os.path import join, split, splitext, exists
from carrot.connection import BrokerConnection

conn = BrokerConnection(hostname="localhost", port=5672,
                        userid="test", password="test",
                        virtual_host="test")

arrot.messaging import Consumer
consumer = Consumer(connection=conn, queue="feed",
                    exchange="feed", routing_key="importer")

def import_feed_callback(message_data, message):
    feed_url = message_data["import_feed"]
    print("Got feed import message for: %s" % feed_url)
    # something importing this feed url
    # import_feed(feed_url)
    message.ack()

consumer.register_callback(import_feed_callback)
consumer.wait() # Go into the consumer loop.
