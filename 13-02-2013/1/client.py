#!/usr/bin/python
import zmq
context = zmq.Context()
socket = context.socket(zmq.REQ)
#socket.connect("tcp://127.0.0.1:5000")
socket.connect("tcp://127.0.0.1:6000")
socket1 = context.socket(zmq.REQ)
socket1.connect("tcp://127.0.0.1:6001")

for i in range(10):
    msg = "msg %s" % i
    ms1= "hello 6001 msg %s" % i
    socket.send(msg)    
    socket1.send(ms1)
    print "Sending", msg
    msg_in = socket.recv()
    msg_in = socket1.recv()
    