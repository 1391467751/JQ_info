#it is used to make log 
import time
import  threading

class MyLog():
    _instance_lock = threading.Lock()

    def __init__(self,filename):
        self.fp = open(filename,"a+")
        self.fp.write("==============start work==================\n")


    @classmethod        
    def  get_instance(cls,filename="./work.log"):
        with MyLog._instance_lock:
            if not hasattr(MyLog , "instance"):
                MyLog.instance = MyLog(filename)
        return MyLog.instance


    def  __del__(self):
        if(self.fp!=None):
            self.fp.write("===================end work================\n")
            self.fp.close()

    def log(self,info,type_id):
        info_log =  "{} : {}# {}".format( time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()), type_id,info)
        self.__print(info_log)
        self.fp.write("{}\n" .format(info_log))
        self.fp.flush()

    def __print(self,info):
        print(info)

    def  print_line(self,info):
        self.__print(info)



if __name__=="__main__":
    ML= MyLog.get_instance("./test.log")
    ML.log("it is a test","Log")
