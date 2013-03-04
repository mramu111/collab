import zmq
import random
import time



context = zmq.Context()

# Socket to send messages on
reciver = context.socket(zmq.PULL)
reciver.bind("tcp://127.0.0.1:5557")
reciver.bind("tcp://127.0.0.1:5558")
reciver.bind("tcp://127.0.0.1:5559")

## Socket with direct access to the sink: used to syncronize start of batch
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
#for task_nbr in range(3):
    # Random workload from 1 to 100 msecs
#    workload = random.randint(1, 100)
#    total_msec += workload
i=1
while True:
    msg=reciver.recv()
    print msg
    i=i+1
    print i
    



# Give 0MQ time to deliver
time.sleep(1)