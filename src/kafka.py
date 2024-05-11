from kafka import KafkaProducer

class kafkaQueue:
    
    def producer(self,message):
        producer = KafkaProducer(bootstrap_servers=['broker1:1234'])

