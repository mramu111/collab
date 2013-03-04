import time
import zmq
#from  multiprocessing import Process


class Worker(Object):
    """
    """
    
    def reader(self):
        """
        """
        context = zmq.Context()
        # Set up a channel to send work
        self.ventilator_send = context.socket(zmq.PUSH)
        self.ventilator_send.bind("tcp://127.0.0.1:5557")
        time.sleep(1)
        
        self.read_file()
    
    
    def bind_all(self):        
       
        
        # Set up a channel to receive work from the ventilator
        self.work_receiver = context.socket(zmq.PULL)
        self.work_receiver.connect("tcp://127.0.0.1:5557")

        # Set up a channel to send result of work to the results reporter
        self.results_sender = context.socket(zmq.PUSH)
        self.results_sender.connect("tcp://127.0.0.1:5558")
        
        # Set up a channel to receive control messages over
        control_receiver = context.socket(zmq.SUB)
        control_receiver.connect("tcp://127.0.0.1:5559")
        control_receiver.setsockopt(zmq.SUBSCRIBE, "")
        
    def listen(self):
         # Set up a poller to multiplex the work receiver and control receiver channels
        poller = zmq.Poller()
        poller.register(self.results_sender, zmq.POLLIN)
        poller.register(control_receiver, zmq.POLLIN)
    
        # Loop and accept messages from both channels, acting accordingly
        while True:
            socks = dict(poller.poll())
    
            # If the message came from work_receiver channel, square the number
            # and send the answer to the results reporter
            if socks.get(self.work_receiver) == zmq.POLLIN:
                work_message = self.work_receiver.recv_json()
                print work_message
                #product = work_message['num'] * work_message['num']
                #answer_message = { 'worker' : wrk_num, 'result' : product }
                self.results_sender.send_json(work_message)
        
            # If the message came over the control channel, shut down the worker.
            if socks.get(control_receiver) == zmq.POLLIN:
                control_message = control_receiver.recv()
                if control_message == "FINISHED":
                    print("Worker %i received FINSHED, quitting!" % wrk_num)
                    break      
                
    def start_service(self):
        """
        """
        self.read_file()
        
    def push(self,etl_msg):                
        self.ventilator_send.send_json(work_message)
        time.sleep(1)

        
        
    def read_file(self):
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
            self.push(data)
            
    def result_manager():
        # Initialize a zeromq context
        context = zmq.Context()
        
        # Set up a channel to receive results
        results_receiver = context.socket(zmq.PULL)
        results_receiver.bind("tcp://127.0.0.1:5558")
        
        # Set up a channel to send control commands
        control_sender = context.socket(zmq.PUB)
        control_sender.bind("tcp://127.0.0.1:5559")
        
        for task_nbr in range(10000):
            result_message = results_receiver.recv_json()
            print "Worker %i answered: %i" % (result_message['worker'], result_message['result'])
        
            # Signal to all workers that we are finsihed
            control_sender.send("FINISHED")
            time.sleep(5)
        
          
    
            
        

class WorkerStartUp(Worker):
    """
    """
    
    def __init__ (self, worker_type, worker_roles=None):
        self.worker_type = worker_type
        self.worker_roles = worker_roles
        
    def start_worker (self):
        worker.bind_all()
        worker.register()
        worker.listen()  



if __name__ == "__main__":
    # Create a pool of workers to distribute work to
    worker_pool = range(3)
    for wrk_num in range(len(worker_pool)):
        Process(target=worker, args=(wrk_num,)).start()

    # Fire up our result manager...
    result_manager = Process(target=result_manager, args=())
    result_manager.start()

    # Start the ventilator!
    ventilator = Process(target=ventilator, args=())
    ventilator.start()

