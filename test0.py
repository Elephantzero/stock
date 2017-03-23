#测试发现timer定时器不会造成总线程的增加，但是会占用CPU的内存
import threading
'''
def test():
    t = threading.current_thread()
    print t.name+'.........',t.ident
    print 'total threding'+'    ',threading.active_count()
    print threading.enumerate()
    print 'test'
    
def circle_test():
    timer = threading.Timer(5,circle_test)
    timer.start()
    test()
    
circle_test()
'''
#测试python多线程
def task1():
    for i in range(10):    
        print 'task1111111111111'
def task2():
    for i in range(10):    
        print 'task2222222222222'
def task3():
    for i in range(10):
        print 'task3333333333333'
        
t1 = threading.Thread(target=task1)
t2 = threading.Thread(target=task2)
t3 = threading.Thread(target=task3)

t1.start()
t2.start()
t3.start()



