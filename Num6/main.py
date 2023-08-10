from configparser import ConfigParser
from confluent_kafka import Producer, Consumer
import json
import random

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
client_config = dict(config_parser['kafka_client'])

sixth_producer = Producer(client_config)

fifth_consumer = Consumer(client_config)
fifth_consumer.subscribe(['main-five'])


def start_service():
    while True:
        msg = fifth_consumer.poll(0.1)
        if msg is None:
            pass
        elif msg.error():
            pass
        else:
            lotto = json.loads(msg.value())
            add_sixthNum(msg.key(), lotto)


def add_sixthNum(order_id, lotto):
    lotto['sixth'] = select_sixthNum(lotto)
    print(json.dumps(lotto))
    sixth_producer.produce('main-six', key=order_id, value=json.dumps(lotto))


def select_sixthNum(lotto):
    j = random.randint(0, 39)
    box = [int(i) for i in range(1, 46)]
    num_to_remove = [lotto['first'], lotto['second'], lotto['third'], lotto['fourth'], lotto['fifth']]
    return [x for x in box if x not in num_to_remove][j]


if __name__ == '__main__':
    start_service()