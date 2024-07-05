import stomp
import logging
import json
from time import sleep
from collections import defaultdict

class NetworkRailConnection(stomp.ConnectionListener):
    """
    Represents a connection to the Network Rail data feeds. A single connection is used for all data feeds (i.e. TRUST and TD)
    This connection object also acts as the listener
    """

    ENDPOINT = 'publicdatafeeds.networkrail.co.uk'
    MAX_ATTEMPTS = 1000 # Number of times we check that we are connected, sleeping 1ms between checks
    PORT = 61618
    subscriber_id = 0

    def __init__(self, username, password, durable = False):
        """ Constructor, takes the username and password used to connect to the NR feeds. Durable connections are supported (See NR documentation) """
        self._connection = stomp.Connection([(self.ENDPOINT, self.PORT)], keepalive=True, heartbeats=(5000, 5000))
        self._username = username
        self._password = password
        self._durable = durable
        self._handlers = defaultdict(set)
        
    def connect(self):
        """
        Connects to the service, waiting to confirm that the connection has been established before returning.
        An exception is raised if the connection is not established after one second
        """
        connect_headers = {
            "username": self._username,
            "passcode": self._password,
            "wait": True,
            }
        
        if self._durable:
            connect_headers["client-id"] = self._username

        self._connection.set_listener("", self)        
        self._connection.connect(**connect_headers)

        attempts = NetworkRailConnection.MAX_ATTEMPTS
        while attempts and not self.is_connected():
            sleep(1)
            attempts -= 1

        if not self._connection.is_connected():
            raise Exception("Failed to connect to", NetworkRailConnection.ENDPOINT)

        logging.info("Connected")

    def subscribe(self, topic):
        """
        Sets up a subscription to a given topic with messages being routed to the listener.
        Note that the connection actually routes all messages to all listeners, so they filter unwanted messages through the
        subscriber ID which is included in messages received
        """
        NetworkRailConnection.subscriber_id += 1
        subscribe_headers = {
            "destination": topic,
            "id": NetworkRailConnection.subscriber_id,
            "ack" : "client-individual" if self._durable else "auto"
            }

        if self._durable:
            subscribe_headers["activemq.subscriptionName"] = self._username + topic

        self._connection.subscribe(**subscribe_headers)

        logging.info(f"Listener subscribed to topic {NetworkRailConnection.subscriber_id} {topic}")

    def is_connected(self):
        """ Indicates if we are still connected, used by the calling application so that it will continue to run if connected """
        return self._connection.is_connected()
    
    def register_handler(self, handler, topic = None):
        """ Registers a handler for the data, so that different handlers can handle different data received for this listener """
        self._handlers[topic].add(handler)

    def on_message(self, frame):
        """
        Callback method called when a message is received
        If the message subscription ID does not match our subscriber ID, we ignore it, otherwise it goes to our handlers
        """
        headers, message_raw = frame.headers, frame.body
        parsed_body = json.loads(message_raw)

        if self._durable:
            # Acknowledging messages is important in client-individual mode
            self._connection.ack(id=headers["ack"], subscription=headers["subscription"])

        handlers = self._handlers[headers["destination"]].union(self._handlers[None])
        for message in parsed_body:
            for handler in handlers:
                handler.process(message)

    def on_error(self, frame):
        """
        Callback method for errors from the NR message queue
        """
        logging.error(f"Received an error on Network Rail feed {self._subscriber_id}: {frame.body}")

    def on_disconnected(self):
        """
        Callback method for disconnection from the NR message queue
        """
        logging.warning("Disconnected from Network Rail feed {self._subscriber_id}")
