from datetime import datetime
from SClassHandler import SClassHandler

class SClassPrinter(SClassHandler):
    def __init__(self, prefix):
        self._prefix = prefix

    def process(self, message):
        print(self._prefix, datetime.now(), message)