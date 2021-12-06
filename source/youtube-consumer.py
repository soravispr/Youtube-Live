from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer('input_sleepy_youtube',
                        bootstrap_server=['ec2-13-233-115-54.ap-south-1.compute.amazonaws.com:9092'],
                        value_deserializer=lambda x: loads(x.secode('utf-8')))

for message in consumer:
    message = message.value
    print(message)