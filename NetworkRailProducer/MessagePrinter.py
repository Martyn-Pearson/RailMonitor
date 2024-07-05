from datetime import datetime
from MessageHandler import MessageHandler

class MessagePrinter(MessageHandler):
    """ Simple Message Handler class which just prints out the message to the console """

    def interested(self, message):
        # We're interested in all messages
        return True

    def handle(self, message):
        """ Simply print out the message with a prefix of a timestamp """
        print(datetime.now(), message)