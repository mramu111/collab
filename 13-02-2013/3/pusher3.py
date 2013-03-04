import zmq
import random
import time
import csv
file_path='/mramu/interface_test/data_files/ramu/data_files/AL/Eligibility/import_200/eligibility-demographics_20121128_000003.txt'
context = zmq.Context()

# Socket to send messages on
sender = context.socket(zmq.PUSH)
sender.connect("tcp://127.0.0.1:5557")
sender.connect("tcp://127.0.0.1:5558")
sender.connect("tcp://127.0.0.1:5559")
#sender.bind("tcp://*:5558")

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


def push(etl_msg):  
    sender.send_json(etl_msg)

f=open(file_path,'r')
r=csv.DictReader(f,delimiter='|')   
        
def _read_file():                
    while True:
        try:
            data=r.next()
            yield data            
        except StopIteration:                
            break  
     
for data in  _read_file():     
    push(data)

#while True :    
#    for i in xrange(1000):        
#        sender.send(str(i))

# Give 0MQ time to deliver
#time.sleep(1)