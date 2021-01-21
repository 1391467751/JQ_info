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
        if(self.db!=None):
            self.cursor.close()
            self.db.close()
            
        try:
            self.db =  pymysql.connect(host =  host,user=user,password=password,database=database,port = port,charset = charset)
        except Exception as e:
            self.Log.log("ERROR : Fail to connect local data base",self.name)
            print(e)
            return 
        if(self.db!=None):
            self.cursor =  self.db.cursor()
        if(self.cursor!=None):
            self.Log.log("Warning : Success init local data base",self.name)

    def execute(self,command):
        if(self.db==None):
            return None
        self.cursor.execute(command)
        self.Log.log("Log : execute cmmand : {}".format(command),self.name)
        return self.cursor.fetchall()

    def insert_multi_data(self,table_name,values , columns = None):
        columns_str = ""
        if(columns!=None):
            columns_str = '( '+ ','.join(columns)+')'
        values_str = ''.join(['(' , '),('.join([','.join(['{}'.format(j) for j in values[i] ]) for i in values] )  , ')']) 
        sql = 'insert into {} {} values {}'.format(table_name , columns_str,value_str)
        return self.execute(sql)
    

    def __del__(self):
        if(self.db!=None):
            self.cursor.close()
            self.db.close()

if __name__=="__main__":
    LD = LocalDataset()    
    LD.init_local_data_set()
