import json
import logging

from NetworkRailConnection import NetworkRailConnection
from NetworkRailListener import NetworkRailListener
from SClassHandler import SClassHandler

from time import sleep

class Application:
    def __init__(self):
        pass

    def run(self):
        try:
            with open("secrets.json") as f:
                feed_username, feed_password = json.load(f)

            connection = NetworkRailConnection(feed_username, feed_password)
            handler = SClassHandler()
            connection.set_listener(NetworkRailListener(connection, handler))

            connection.connect_and_subscribe()

            while connection.is_connected():
                sleep(1)
        except Exception as ex:
            print(ex)
            logging.exception(ex)

if __name__ == "__main__":
    logging.basicConfig(filename="sclass.log", level=logging.INFO)
    Application().run()
