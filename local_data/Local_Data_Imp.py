# local_data_imp
import sys
sys.path.append("..")

import pymysql
from tool.mylog  import MyLog as  Mylog

class LocalDataset():
    def __init__(self):
        self.db = None;
        self.cursor = None;
        self.Log = Mylog.get_instance()
        self.name = "Local DataImp"

    def init_local_data_set(self,host="localhost",user="root",password="None",database = "tmp",port=3306,charset='utf8'):
        self.db =  pymysql.connect(host =  host,user=user,password=password,database=database,port = port,charset = charset)
        if(self.db!=None):
            self.cursor =  self.db.cursor()
        if(self.cursor!=None):
            self.Log.log("Success init local data base",self.name)

    def __del__(self):
        if(self.db!=None):
            self.cursor.close()
            self.db.close()

if __name__=="__main__":
    LD = LocalDataset()    
    LD.init_local_data_set()
