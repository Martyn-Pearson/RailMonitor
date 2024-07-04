import json
import logging

from NetworkRailConnection import NetworkRailConnection
from NetworkRailListener import NetworkRailListener
from SClassPrinter import SClassPrinter

from time import sleep

class Application:
    def __init__(self):
        pass

    def run(self):
        try:
            with open("secrets.json") as f:
                feed_username, feed_password = json.load(f)

            connection = NetworkRailConnection(feed_username, feed_password)
            connection.connect()

            listener = NetworkRailListener(connection)
            listener.register_handler(SClassPrinter("TD"))
            connection.subscribe("/topic/TD_ALL_SIG_AREA", listener)

            listener = NetworkRailListener(connection)
            listener.register_handler(SClassPrinter("TRUST"))
            connection.subscribe("/topic/TRAIN_MVT_ALL_TOC", listener)

            while connection.is_connected():
                sleep(1)
        except Exception as ex:
            print(ex)
            logging.exception(ex)

if __name__ == "__main__":
    logging.basicConfig(filename="networrailproducer.log", level=logging.INFO)
    Application().run()
