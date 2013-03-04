import time
import zmq
from  multiprocessing import Process
import sys
import simplejson
import csv

def ventilator():
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to send work
    ventilator_send = context.socket(zmq.PUSH)
    ventilator_send.bind("tcp://127.0.0.1:5557")

    # Give everything a second to spin up and connect
    time.sleep(1)   
    
    file_path='/mramu/interface_test/data_files/ramu/data_files/Eligibility/import/jiva-eligibility-demographics_20110706_151810.csv'
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
        ventilator_send.send(simplejson.dumps(data)) 
    ventilator_send.send(simplejson.dumps("FINISHED"))

    time.sleep(1)

    
def worker(wrk_num):
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to receive work from the ventilator
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect("tcp://127.0.0.1:5557")

    # Set up a channel to send result of work to the results reporter
    results_sender = context.socket(zmq.PUSH)
    results_sender.connect("tcp://127.0.0.1:5558")

    # Set up a channel to receive control messages over
    control_receiver = context.socket(zmq.SUB)
    control_receiver.connect("tcp://127.0.0.1:5559")
    control_receiver.setsockopt(zmq.SUBSCRIBE, "")

    # Set up a poller to multiplex the work receiver and control receiver channels
    poller = zmq.Poller()
    poller.register(work_receiver, zmq.POLLIN)
    poller.register(control_receiver, zmq.POLLIN)
    
    time.sleep(1)

    # Loop and accept messages from both channels, acting accordingly
    while True:
        socks = dict(poller.poll(1000))        
        if socks:
            # If the message came from work_receiver channel, square the number
            # and send the answer to the results reporter
            if socks.get(work_receiver) == zmq.POLLIN:
                work_message = simplejson.loads(work_receiver.recv())          
                answer_message = { 'worker' : wrk_num, 'result' : work_message }
                results_sender.send(simplejson.dumps(answer_message))
        
            # If the message came over the control channel, shut down the worker.
            if socks.get(control_receiver) == zmq.POLLIN:
                control_message = simplejson.loads(control_receiver.recv())
                if control_message == "FINISHED":                    
                    print("Worker %i received FINSHED, quitting!" % wrk_num)
                    break
                    
                    
                    
        else:
            print "Time out"
            sys.exit()
            #return 

def result_manager():
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to receive results
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind("tcp://127.0.0.1:5558")

    # Set up a channel to send control commands
    control_sender = context.socket(zmq.PUB)
    control_sender.bind("tcp://127.0.0.1:5559")

    for task_nbr in range(100):
        result_message = simplejson.loads(results_receiver.recv())        
        print "\nWorker %i answered: %s" % (result_message['worker'], result_message['result'])


        # Signal to all workers that we are finsihed
        control_sender.send(simplejson.dumps("FINISHED"))
        time.sleep(5)
        
if __name__ == "__main__":    
    # Create a pool of workers to distribute work to
    worker_pool = range(5)
    for wrk_num in range(len(worker_pool)):
        Process(target=worker, args=(wrk_num,)).start()

    # Fire up our result manager...
    result_manager = Process(target=result_manager, args=())
    result_manager.start()

    # Start the ventilator!
    ventilator = Process(target=ventilator, args=())
    ventilator.start()

