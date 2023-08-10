import json
import pickle

from main import Lotto, LottoOrder
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer

config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
producer_config = dict(config_parser['kafka_client'])
consumer_config = dict(config_parser['kafka_client'])
consumer_config.update(config_parser['consumer'])
lotto_producer = Producer(producer_config)

lotto_box = {}

def order_lottos(count):
    order = LottoOrder(count)
    lotto_box[order.id] = order

    # order_id와 LottoOrder객체를 저장하는 피클파일 생성
    with open('lotto_box.p', 'wb') as file:
        pickle.dump(lotto_box, file)

    for _ in range(count):
        new_lotto = Lotto()
        new_lotto.order_id = order.id
        lotto_producer.produce('main', key=order.id, value=new_lotto.toJSON())
    lotto_producer.flush()
    return order.id

def get_order(order_id):
    while True:
        try:
            with open('lotto_box.p', 'rb') as file:
                lotto_box = pickle.load(file)
        except:
            pass
        # order_id에 해당하는 로또 번호가 마지막 컨슈머로부터 poll 되서 lotto_box.p에 저장되어 있다면 밑 로직 실행
        if (order_id in lotto_box.keys()) and len(lotto_box[order_id].get_lottos()) == lotto_box[order_id].get_count():
            order = lotto_box[order_id]
            return order.get_lottos()
        else:
            pass

def load_orders():
    lotto_consumer = Consumer(consumer_config)
    lotto_consumer.subscribe(['main-six'])
    while True:
        event = lotto_consumer.poll(1.0)
        if event is None:
            pass
        elif event.error():
            print(f'Bummer - {event.error()}')
        else:
            lotto = json.loads(event.value())
            add_lotto(lotto['order_id'], lotto)
            

def add_lotto(order_id, lotto):
    with open('lotto_box.p', 'rb') as file:
        lotto_box = pickle.load(file)

    if order_id in lotto_box.keys():
        order = lotto_box[order_id]
        order.add_lotto(lotto)
    
    with open('lotto_box.p', 'wb') as file:
        pickle.dump(lotto_box, file)