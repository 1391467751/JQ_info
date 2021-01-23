from tool.mylog import MyLog
from remote_data.Remote_Data_Imp import remote_dataset
from local_data.Local_Data_Imp import LocalDataset

import datetime
import jqdatasdk as jqs

class MyDataset():
    def __init__(self):
        self.log = MyLog.get_instance()
        self.ld =  LocalDataset()
        self.rd =  remote_dataset()

    def init(self):
        self.ld.init_local_data_set(host = "cdb-mqzvz536.bj.tencentcdb.com" ,port =10146 , user="root" , database="stock_test")
        self.rd.init()

    def get_stock_data_(self,code , start_date ,end_date):
        pass
