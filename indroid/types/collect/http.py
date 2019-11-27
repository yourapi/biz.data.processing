"""Get content over HTTP, could be RESTful, SOAP, HTML or whichever protocol."""
import os, re
import mechanize
from os.path import join, split, splitext, exists

class Browser(object):
    def __init__(self, address):
        self._address = address

def test():
    url = "https://odin4.kpn.net/reports/"
    #url = "https://odin4.kpn.net/reports/odin_dump-2012-10-03.zip"
    br = mechanize.Browser()
    br.add_client_certificate("odin4.kpn.net", r"P:\odin.pem", r"p:\odin.pub")
    print br.open(url)
    print br.viewing_html()
    print br.response()

if __name__ == '__main__':
    test()