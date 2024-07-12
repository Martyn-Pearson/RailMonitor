import json
import logging

from MessageHandler import MessageHandler

class MovementProducer(MessageHandler):
    TOPIC = "movement-events"

    """ Reads movement messages (code 0003) from the TD feed and writes them to the Kafka movement-events topic """

    def __init__(self, producer):
        """ Constructor, passed the Kafka producer that we'll use to publish to the topic """
        self._producer = producer

    def interested(self, message):
        """ Only interested if the message is a CA message """
        return message["header"]["msg_type"] == "0003"
        
    def handle(self, message):
        """ Method to be overridden to handle the message as appropriate """
        self._producer.produce(self.TOPIC, key = message["body"]["loc_stanox"], value = json.dumps(message["body"]), callback = MovementProducer.kafka_ack)

    @staticmethod
    def kafka_ack(err, msg):
        """ Callback message that we use to log any errors delivering to Kafka """
        if err is not None:
            logging.error(f"BerthProducer failed to deliver message {msg}, error {err}")