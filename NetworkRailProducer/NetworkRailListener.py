import stomp
import logging
import json

class NetworkRailListener(stomp.ConnectionListener):
    def __init__(self, connection):
        self._connection = connection
        self._durable = False
        self._handlers = []
        self._subscriber_id = ""

    def set_durable(self, durable):
        self._durable = durable

    def set_subscriber_id(self, id):
        self._subscriber_id = str(id)

    def register_handler(self, handler):
        self._handlers.append(handler)

    def on_message(self, frame):
        headers, message_raw = frame.headers, frame.body
        parsed_body = json.loads(message_raw)

        if self._durable:
            # Acknowledging messages is important in client-individual mode
            self._connect.ack(id=headers["ack"], subscription=headers["subscription"])

        if headers["subscription"] == self._subscriber_id:
            for message in parsed_body:
                for handler in self._handlers:
                    handler.process(message)

    def on_error(self, frame):
        logging.error(f"Received an error on Network Rail feed {self._subscriber_id}: {frame.body}")

    def on_disconnected(self):
        logging.warning("Disconnected from Network Rail feed {self._subscriber_id}")
