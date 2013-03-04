#!/usr/bin/python


import zmq
import simplejson
from time import sleep
context = zmq.Context()
transformer_sender = context.socket(zmq.PUSH)
transformer_sender.bind("tcp://127.0.0.1:6000")
transformer_sender.bind("tcp://127.0.0.1:6001")

transformer_receiver1 = context.socket(zmq.PULL)
transformer_receiver1.connect("tcp://127.0.0.1:6000")

transformer_receiver2 = context.socket(zmq.PULL)
transformer_receiver2.connect("tcp://127.0.0.1:6001")

for i in xrange(1000):
    for i in range(6001,6003):
        reciver_uri="tcp://127.0.0.1:%s" %i
        transformer_sender.bind(reciver_uri)        
        transformer_sender.send_json(i)   
    #sleep(1)
    
while True:
    msg1 = transformer_receiver1.recv()
    sleep(1)
    msg2 = transformer_receiver2.recv()
    print msg1
    print msg2
    
    
#for i in range(2):    
#    msg2 = transformer_receiver2.recv()
#    print msg2
    
