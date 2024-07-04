import stomp
import logging
import json

class NetworkRailListener(stomp.ConnectionListener):
    def __init__(self, connection, handler, durable = False):
        self._connection = connection
        self._handler = handler
        self._durable = durable

    def on_message(self, frame):
        headers, message_raw = frame.headers, frame.body
        parsed_body = json.loads(message_raw)

        if self._durable:
            # Acknowledging messages is important in client-individual mode
            self._connect.ack(id=headers["ack"], subscription=headers["subscription"])

        if "TD_" in headers["destination"]:
            self._handler.process(parsed_body)
        else:
            logging.warning("Unknown destination: ", headers["destination"])

    def on_error(self, frame):
        logging.error(f"Received an error on Network Rail feed: {frame.body}")

    def on_disconnected(self):
        logging.warning("Disconnected from Network Rail feed")

    def is_durable(self):
        return self._durable