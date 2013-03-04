import time
import zmq
from  multiprocessing import Process
import sys

def ventilator():
    # Initialize a zeromq context
    self.context = zmq.Context()

    # Set up a channel to send work
    ventilator_send = context.socket(zmq.PUSH)
    ventilator_send.bind("tcp://127.0.0.1:5557")

    # Give everything a second to spin up and connect
    time.sleep(1)

    # Send the numbers between 1 and ten thousand as work messages
    for num in range(10):
        work_message = { 'num' : num }
        ventilator_send.send_json(work_message)

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
        socks = dict(poller.poll())
        print socks
        if socks:
            # If the message came from work_receiver channel, square the number
            # and send the answer to the results reporter
            if socks.get(work_receiver) == zmq.POLLIN:
                work_message = work_receiver.recv_json()
                #print work_message['num']
                product = work_message['num'] * work_message['num']
                answer_message = { 'worker' : wrk_num, 'result' : product }
                results_sender.send_json(answer_message)
        
            # If the message came over the control channel, shut down the worker.
            if socks.get(control_receiver) == zmq.POLLIN:
                control_message = control_receiver.recv()
                if control_message == "FINISHED":
                    print("Worker %i received FINSHED, quitting!" % wrk_num)
                    self.context=None
                    sys.exit()
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

    for task_nbr in range(1000):
        result_message = results_receiver.recv_json()
        print "Worker %i answered: %i" % (result_message['worker'], result_message['result'])

        # Signal to all workers that we are finsihed
        control_sender.send("FINISHED")
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

