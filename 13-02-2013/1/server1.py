#!/usr/bin/python
import zmq
from time import sleep

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://127.0.0.1:6000")
socket.bind("tcp://127.0.0.1:6001")
 
while True:
    msg = socket.recv()    
    print "Got", msg
    socket.send(msg)