from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

third_producer = Producer(client_config)

second_consumer = Consumer(client_config)
second_consumer.subscribe(['main-two'])


def start_service():
    while True:
        msg = second_consumer.poll(0.1)
        if msg is None:
            pass
        elif msg.error():
            pass
        else:
            lotto = json.loads(msg.value())
            add_thirdNum(msg.key(), lotto)


def add_thirdNum(order_id, lotto):
    lotto['third'] = select_thirdNum(lotto)
    third_producer.produce('main-three', key=order_id, value=json.dumps(lotto))


def select_thirdNum(lotto):
    j = random.randint(0, 42)
    box = [int(i) for i in range(1, 46)]
    num_to_remove = [lotto['first'], lotto['second']]
    return [x for x in box if x not in num_to_remove][j]


if __name__ == '__main__':
    start_service()