from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

fourth_producer = Producer(client_config)

third_consumer = Consumer(client_config)
third_consumer.subscribe(['main-three'])


def start_service():
    while True:
        msg = third_consumer.poll(0.1)
        if msg is None:
            pass
        elif msg.error():
            pass
        else:
            lotto = json.loads(msg.value())
            add_fourthNum(msg.key(), lotto)


def add_fourthNum(order_id, lotto):
    lotto['fourth'] = select_fourthNum(lotto)
    fourth_producer.produce('main-four', key=order_id, value=json.dumps(lotto))


def select_fourthNum(lotto):
    j = random.randint(0, 41)
    box = [int(i) for i in range(1, 46)]
    num_to_remove = [lotto['first'], lotto['second'], lotto['third']]
    return [x for x in box if x not in num_to_remove][j]


if __name__ == '__main__':
    start_service()