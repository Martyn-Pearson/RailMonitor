import json
import logging

from NetworkRailConnection import NetworkRailConnection
from MessagePrinter import MessagePrinter

from time import sleep

class Application:
    """
    Application class which encapsulates connecting to the Network Rail feeds and Kafka
    """
    def __init__(self):
        pass

    def run(self):
        try:
            with open("secrets.json") as f:
                feed_username, feed_password = json.load(f)

            connection = NetworkRailConnection(feed_username, feed_password)
            connection.connect()
            connection.register_handler(MessagePrinter(), "TD_ALL_SIG_AREA")
            connection.subscribe("/topic/TD_ALL_SIG_AREA")
            connection.subscribe("/topic/TRAIN_MVT_ALL_TOC")

            while connection.is_connected():
                sleep(1)
        except Exception as ex:
            print(ex)
            logging.exception(ex)

if __name__ == "__main__":
    logging.basicConfig(filename="networkrailproducer.log", level=logging.INFO)
    Application().run()
