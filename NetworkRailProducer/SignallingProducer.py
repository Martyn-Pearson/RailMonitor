import json
import logging

from MessageHandler import MessageHandler

class SignallingProducer(MessageHandler):
    TOPIC = "signal-events"

    """ Reads SF messages from the TD feed and writes them to the Kafka signal-events topic """

    def __init__(self, producer):
        """ Constructor, passed the Kafka producer that we'll use to publish to the topic """
        self._producer = producer

    def interested(self, message):
        """ Only interested if the message is a CA message """
        return "SF_MSG" in message
        
    def handle(self, message):
        """ Method to be overridden to handle the message as appropriate """
        self._producer.produce(self.TOPIC, key = message["SF_MSG"]["area_id"], value = json.dumps(message["SF_MSG"]), callback = SignallingProducer.kafka_ack)

    @staticmethod
    def kafka_ack(err, msg):
        """ Callback message that we use to log any errors delivering to Kafka """
        if err is not None:
            logging.error(f"SignallingProducer failed to deliver message {msg}, error {err}")