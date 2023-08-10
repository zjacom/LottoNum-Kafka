import json
import uuid

class Lotto:
    def __init__(self):
        self.order_id = ''
        self.first = ''
        self.second = ''
        self.third = ''
        self.fourth = ''
        self.fifth = ''
        self.sixth = ''

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=False, indent=4)
    def __str__(self):
        return json.dumps(self.__dict__)



class LottoOrder:
    def __init__(self, count):
        self.id = str(uuid.uuid4().int)
        self.count = count
        self.lottos = []

    def add_lotto(self, lotto):
        self.lottos.append(lotto)

    def get_lottos(self):
        return self.lottos
    
    def get_count(self):
        return self.count

    def __str__(self):
        return json.dumps(self.__dict__)