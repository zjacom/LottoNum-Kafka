import threading
import main_service

def launch_consumer():
    t = threading.Thread(target=main_service.load_orders)
    t.start()

if __name__ == '__main__':
    launch_consumer()