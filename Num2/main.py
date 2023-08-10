from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

second_producer = Producer(client_config)

first_consumer = Consumer(client_config)
first_consumer.subscribe(['main-one'])


def start_service():
    while True:
        msg = first_consumer.poll(0.1)
        if msg is None:
            pass
        elif msg.error():
            pass
        else:
            lotto = json.loads(msg.value())
            add_secondNum(msg.key(), lotto)


def add_secondNum(order_id, lotto):
    lotto['second'] = select_secondNum(lotto)
    second_producer.produce('main-two', key=order_id, value=json.dumps(lotto))


def select_secondNum(lotto):
    j = random.randint(0, 43)
    box = [int(i) for i in range(1, 46)]
    num_to_remove = [lotto['first']]
    return [x for x in box if x not in num_to_remove][j]


if __name__ == '__main__':
    start_service()