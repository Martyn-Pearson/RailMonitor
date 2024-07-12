import json
import logging
import socket
from time import sleep

from confluent_kafka import Producer

from NetworkRailConnection import NetworkRailConnection
from MessagePrinter import MessagePrinter
from BerthProducer import BerthProducer
from SignallingProducer import SignallingProducer
from MovementProducer import MovementProducer

class Application:
    """
    Application class which encapsulates connecting to the Network Rail feeds and Kafka
    """
    KAFKA_POLL_INTERVAL = 500

    def __init__(self):
        pass

    def run(self):
        try:
            with open("secrets.json") as f:
                feed_username, feed_password = json.load(f)

            kafka_config = {"bootstrap.servers" : "localhost:9092",
                            "client.id" : socket.gethostname()} 

            kafka_producer = Producer(kafka_config)

            # Connect to the Network Rail feeds, register the handlers and subscribe
            connection = NetworkRailConnection(feed_username, feed_password)
            connection.connect()
#            connection.register_handler(MessagePrinter(), "TRAIN_MVT_ALL_TOC")
            connection.register_handler(BerthProducer(kafka_producer), "TD_ALL_SIG_AREA")
            connection.register_handler(SignallingProducer(kafka_producer), "TD_ALL_SIG_AREA")
            connection.register_handler(MovementProducer(kafka_producer), "TRAIN_MVT_ALL_TOC")

            connection.subscribe("/topic/TD_ALL_SIG_AREA")
            connection.subscribe("/topic/TRAIN_MVT_ALL_TOC")

            next_poll = Application.KAFKA_POLL_INTERVAL
            while connection.is_connected():
                next_poll -= 1
                if next_poll <= 0:
                    next_poll = Application.KAFKA_POLL_INTERVAL
                    kafka_producer.poll(1)
                else:
                    sleep(1)
        except Exception as ex:
            print(ex)
            logging.exception(ex)
            

if __name__ == "__main__":
    logging.basicConfig(filename="networkrailproducer.log", level=logging.INFO)
    Application().run()
