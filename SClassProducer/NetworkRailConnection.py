import stomp
import logging

class NetworkRailConnection:
    ENDPOINT = 'publicdatafeeds.networkrail.co.uk'
    PORT = 61618
    TOPIC = "/topic/TD_ALL_SIG_AREA"

    def __init__(self, username, password):
        self._connection = stomp.Connection([(self.ENDPOINT, self.PORT)], keepalive=True, heartbeats=(5000, 5000))
        self._username = username
        self._password = password
        self._listener = None
        
    def set_listener(self, listener):
        self._listener = listener
        self._connection.set_listener("", listener)

    def connect_and_subscribe(self):
        assert(self._listener)
        connect_headers = {
            "username": self._username,
            "passcode": self._password,
            "wait": True,
            }
        subscribe_headers = {
            "destination": self.TOPIC,
            "id": 1,
            "ack" : "client-individual" if self._listener.is_durable() else "auto"
            }
        if self._listener.is_durable():
            connect_headers["client-id"] = self._username
            subscribe_headers["activemq.subscriptionName"] = self._username + self.TOPIC

        self._connection.connect(**connect_headers)
        logging.info("Connected")
        self._connection.subscribe(**subscribe_headers)
        logging.info("Subscribed")

    def is_connected(self):
        return self._connection.is_connected()