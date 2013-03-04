class A(object):
    def add(self):
        print "hello"
        
class B(object):
    def div(self):
        print "hello2"
        
class C(object):
    def __init__(self):        
        a=A()
        a.add()

if __name__=="__main__":
    c=C()

