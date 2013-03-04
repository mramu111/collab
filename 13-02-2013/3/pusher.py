import zmq
import random
import time

context = zmq.Context()

# Socket to send messages on
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:5557")
sender.bind("tcp://*:5558")

# Socket with direct access to the sink: used to syncronize start of batch
#sink = context.socket(zmq.PUSH)
#sink.connect("tcp://localhost:5558")

print "Press Enter when the workers are ready: "
_ = raw_input()
print "Sending tasks to workers"

# The first message is "0" and signals start of batch
#sink.send('0')

# Initialize random number generator
#random.seed()

# Send 100 tasks
#total_msec = 0
##or task_nbr in range(3):
    ## Random workload from 1 to 100 msecs
    #workload = random.randint(1, 1000)
    #total_msec += workload

    #sender.send(str(workload))

#print "Total expected cost: %s msec" % total_msec
while True :    
    for i in xrange(1000):        
        sender.send(str(i))

# Give 0MQ time to deliver
#time.sleep(1)