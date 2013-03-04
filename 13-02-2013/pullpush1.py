import zmq
from time import sleep
from multiprocessing import Process

# TODO: this only works on Linux
PORT = 'tcp://127.0.0.1:4444'


def start_send():
    ctx = zmq.Context()
    task_sender = ctx.socket(zmq.PUSH)
    task_sender.bind(PORT)
    task_sender.send('hello')
    sleep(1)
    task_sender.send('world')


def start_receive():
    ctx = zmq.Context()
    task_recv = ctx.socket(zmq.PULL)
    task_recv.connect(PORT)
    msg = task_recv.recv()
    print(msg)


if __name__ == '__main__':
    p1 = Process(target=start_send)
    p2 = Process(target=start_receive)
    p3 = Process(target=start_receive)
    p2.start()
    p1.start()    
    p3.start()