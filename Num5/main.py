from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

fifth_producer = Producer(client_config)

fourth_consumer = Consumer(client_config)
fourth_consumer.subscribe(['main-four'])


def start_service():
    while True:
        msg = fourth_consumer.poll(0.1)
        if msg is None:
            pass
        elif msg.error():
            pass
        else:
            lotto = json.loads(msg.value())
            add_fifthNum(msg.key(), lotto)


def add_fifthNum(order_id, lotto):
    lotto['fifth'] = select_fifthNum(lotto)
    fifth_producer.produce('main-five', key=order_id, value=json.dumps(lotto))


def select_fifthNum(lotto):
    j = random.randint(0, 40)
    box = [int(i) for i in range(1, 46)]
    num_to_remove = [lotto['first'], lotto['second'], lotto['third'], lotto['fourth']]
    return [x for x in box if x not in num_to_remove][j]


if __name__ == '__main__':
    start_service()