from kafka import KafkaProducer
import json

class KafkaQueue:
    """
    Produce and consume messages to kafka
    """
    
    def producer(self,message: json)->None:
        """
        Produce json messages to kafka

        Args:
            message: incoming json message
        """
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.send(message)

