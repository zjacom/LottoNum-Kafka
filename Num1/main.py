from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

first_producer = Producer(client_config)

lotto_consumer = Consumer(client_config)
lotto_consumer.subscribe(['main'])


def start_service():
    while True:
        msg = lotto_consumer.poll(0.1)
        if msg is None:
            pass
        elif msg.error():
            pass
        else:
            lotto = json.loads(msg.value())
            add_firstNum(msg.key(), lotto)


def add_firstNum(order_id, lotto):
    lotto['first'] = select_firstNum()
    first_producer.produce('main-one', key=order_id, value=json.dumps(lotto))


def select_firstNum():
    j = random.randint(0, 44)
    return [int(i) for i in range(1, 46)][j]

if __name__ == '__main__':
    start_service()