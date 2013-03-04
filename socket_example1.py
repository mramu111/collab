# Socket to send messages to
#transformer_send = context.socket(zmq.PUSH)
#transformer_send.connect("tcp://localhost:5558")

#loader_recv = context.socket(zmq.PULL)
#loader_recv.connect("tcp://*:5558")

#if socks.get(transformer_recv2) == zmq.POLLIN:
    #msg = transformer_recv2.recv(flags=zmq.NOBLOCK)
    #print msg
#if socks.get(transformer_recv3) == zmq.POLLIN:
    #msg = transformer_recv3.recv(flags=zmq.NOBLOCK)
    #print msg

#socket.send('request #%d' % (reqs))    


#for data in read_file():        
    #reader.send(simplejson.dumps(data))   

    
    
    #record=simplejson.loads(transformer_recv.recv())
    #if record:
        #record=transformer_send.send(simplejson.dumps(record))

   
    #msg=simplejson.loads(loader_recv.recv())

    #print msg






import zmq
import csv
import json
import simplejson
context = zmq.Context()

class Sender(object): 

     def __init__ (self, task_key, socket_type):
          self.task_key = task_key
          self.socket_type = socket_type
          self.endpoint_uris = None
          self._out_socket = None

     
     
     @property
     def out_socket(self):
          if self._out_socket:
               return self._out_socket
          context = zmq.Context()
          self._out_socket = context.socket(self.socket_type)
          self._out_socket.setsockopt(zmq.LINGER, 0)
          return self._out_socket
     
     def connect_endpoints (self, uris=None):
          """
          Create a client connection to a sequence of uris.

          If no ``uris`` are provided, use ``self.endpoint_uri``.
  
          Overwrite existing self.endpoint_uris if uri is provided.
          """
          if uris:
               self.endpoint_uris = uris
          for uri in self.endpoint_uris:
               self.out_socket.connect(uri)
               
     def send (self, msg):
          """
          Send data on pre-established connections.
          """
          self.out_socket.send(msg)


class SocketExample(object):
    
    
     def start_reader_wokers(self):
          
          #Sender()
          self.reader_publisher1=context.socket(zmq.PUSH)
          self.reader_publisher1.setsockopt(zmq.LINGER, 0)
          
          self.reader_publisher2=context.socket(zmq.PUSH)
          self.reader_publisher2.setsockopt(zmq.LINGER, 0)
          
          self.transofmer_senders=[self.reader_publisher1.bind("tcp://*:6001"),
                        self.reader_publisher2.bind("tcp://*:6002"),
                        ]              
               
               
               
    
     def start_transformers_workers(self):
          self.transformer_listener1=context.socket(zmq.PULL)          
          self.transformer_listener2=context.socket(zmq.PULL)          
          
          self.transformer_recivers=[self.transformer_listener1.connect("tcp://*:6001"),
                                     self.transformer_listener2.connect("tcp://*:6002")
                                     ]          
          
          
          
          self.loader_publisher1=context.socket(zmq.PUSH)
          self.loader_publisher1.setsockopt(zmq.LINGER, 0)
          
          self.loader_publisher2=context.socket(zmq.PUSH)
          self.loader_publisher2.setsockopt(zmq.LINGER, 0)
          
          self.transofmer_senders=[self.loader_publisher1.bind("tcp://*:7001"),
                        self.loader_publisher2.bind("tcp://*:7002"),]
          
          
        
    
     def start_loader_workers(self):         
          self.loader_reciver1=context.socket(zmq.PULL)          
          self.loader_reciver2=context.socket(zmq.PULL)          
          
          self.loader_recivers=[self.loader_reciver1.connect("tcp://*:7001"),
                                     self.loader_reciver2.connect("tcp://*:7002")
                                     ]          
          
    
     def start_workers(self):
         
         
          self.start_reader_wokers()    
          self.start_transformers_workers()
          self.start_loader_workers()  
          
          
          self.poller = zmq.Poller()    
          self.poller.register(self.transformer_sender1, zmq.POLLIN)           
          self.poller.register(self.transformer_sender2, zmq.POLLIN)
          self.poller.register(self.transformer_sender3, zmq.POLLIN)
          
          self.loader_poller = zmq.Poller()
          
          self.loader_poller.register(self.loader_sender1, zmq.POLLIN)   
          self.loader_poller.register(self.loader_sender2, zmq.POLLIN)
          self.loader_poller.register(self.loader_sender3, zmq.POLLIN)
         
        
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
              #print data
               self.push(data)
          self.push('DONE')
             
     def push(self,record):      
          for sender in self.senders:            
               if record=='DONE':
                    return
               #sender.send(simplejson.dumps(record))
               else:
                    print record
                    sender.send(simplejson.dumps(record))
                    
         #self.transformer_level()
                     
     def loader_push(self,record):       
          if record=='DONE':
               return
          else:
               print "\nrecord at push" , record.get('ENROLL_ID')
               for loader_sender in self.loader_senders:             
                    if record:                
                         loader_sender.send(simplejson.dumps(record))
                         self.loader_level()                
                 
                 
                     
     def transformer_level(self):        
         
          listening=True    
          self.records=[]
          while listening:         
               socks = dict(self.poller.poll())             
               if socks.get(self.transformer_sender1) == zmq.POLLIN:
                    msg =simplejson.loads(self.transformer_recv1.recv())    
                    #print msg
                    self.records.append(msg)   
                    self.loader_push(msg)
           
               if socks.get(self.transformer_sender2) == zmq.POLLIN:
                    msg =simplejson.loads(self.transformer_recv2.recv())  
                    #print msg
                    self.records.append(msg)
                    self.loader_push(msg)
                  
               if socks.get(self.transformer_sender3) == zmq.POLLIN:
                    msg =simplejson.loads(self.transformer_recv3.recv())                
                    self.records.append(msg)
                    self.loader_push(msg)
                    #print msg
               else: 
                    self.loader_push('DONE')
                    listening=False
                    return None
                 
         #for record in self.records:
             #print record
             #self.loader_push(record)
         
         
 

    
     def loader_level(self):       
         
          listening=True            
          while listening:         
               socks = dict(self.loader_poller.poll())             
               if socks.get(self.loader_recv1) == zmq.POLLIN:
                    msg =simplejson.loads(self.loader_recv1.recv())
                
                #print msg.get('ENROLL_ID')
                #print 'loader 1'   
                
        
               elif socks.get(self.loader_recv2) == zmq.POLLIN:
                    msg =simplejson.loads(self.loader_recv2.recv())
                   
                #print msg
                #print '\n loader 2'
                #print msg.get('ENROLL_ID')      
                
                
               elif socks.get(self.loader_recv3) == zmq.POLLIN:
                    msg =simplejson.loads(self.loader_recv3.recv())
                    print msg
                    print '\n loader 3'
                    print msg.get('ENROLL_ID')
                
               else:
                    return None


 
if __name__=='__main__':    
     socket_example=SocketExample()    
     socket_example.start_workers()
     socket_example.read_file()
     socket_example.transformer_level()
     socket_example.loader_level()
