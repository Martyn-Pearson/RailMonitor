import stomp
import logging
from time import sleep

class NetworkRailConnection:
    ENDPOINT = 'publicdatafeeds.networkrail.co.uk'
    MAX_ATTEMPTS = 1000 # Number of times we check that we are connected, sleeping 1ms between checks
    PORT = 61618
    subscriber_id = 0

    def __init__(self, username, password, durable = False):
        self._connection = stomp.Connection([(self.ENDPOINT, self.PORT)], keepalive=True, heartbeats=(5000, 5000))
        self._username = username
        self._password = password
        self._durable = durable
        
    def connect(self):
        connect_headers = {
            "username": self._username,
            "passcode": self._password,
            "wait": True,
            }
        if self._durable:
            connect_headers["client-id"] = self._username
        self._connection.connect(**connect_headers)

        attempts = NetworkRailConnection.MAX_ATTEMPTS
        while attempts and not self._connection.is_connected():
            sleep(1)
            attempts -= 1

        if not self._connection.is_connected():
            raise Exception("Failed to connect to", NetworkRailConnection.ENDPOINT)

        logging.info("Connected")

    def subscribe(self, topic, listener):
        NetworkRailConnection.subscriber_id += 1
        subscribe_headers = {
            "destination": topic,
            "id": NetworkRailConnection.subscriber_id,
            "ack" : "client-individual" if self._durable else "auto"
            }

        if self._durable:
            subscribe_headers["activemq.subscriptionName"] = self._username + topic
            listener.set_durable(True)

        listener.set_subscriber_id(NetworkRailConnection.subscriber_id)
        self._connection.set_listener(topic, listener)
        self._connection.subscribe(**subscribe_headers)

        logging.info("Subscribed to topic " + topic)

    def is_connected(self):
        return self._connection.is_connected()