from kafka import KafkaProducer
import json

class ProducerFactory:

    def __init__(self, bootstrap_server):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer=lambda m: json.dumps(m).encode('ascii'));

    def getJsonProducer(self, topic, columns, keyColumn=None):
        return JsonProducer(topic, columns, keyColumn, self.producer) 

class JsonProducer:
    
    def __init__(self, topic, columns, keyColumn, producer):
        self.producer = producer
        self.topic = topic
        self.keyColumn = keyColumn
        self.columns = columns

    def sendMessage(self, values):
        message = dict(zip(self.columns, values))
        if self.keyColumn:
            key = message.get(self.keyColumn)
            key = key.encode('utf-8')
        else:
            key = None

        self.producer.send(self.topic, message, key=key)
        self.producer.flush()
        print("Message sent \nbody: {} \nkey: {}".format(message, key))
