from tool.mylog import MyLog
from remote_data.Remote_Data_Imp import remote_dataset
from local_data.Local_Data_Imp import LocalDataset

import datetime
import jqdatasdk as jqs
import time

class MyDataset():
    def __init__(self):
        self.log = MyLog.get_instance()
        self.ld =  LocalDataset()
        self.rd =  remote_dataset()
        self.name = "My Dataset"

    def init(self):
        self.ld.init_local_data_set(host = "cdb-mqzvz536.bj.tencentcdb.com" ,port =10146 , user="root" , database="stock_test")
        self.rd.init()
        self.log.log("Log : Dataset is initiated",self.name)

    def get_stock_data_start_end(self,code , start_date=None,end_date=None):
        '''
            interface  get code price form start to end
            start and end should be  "%Y-%m-%d"
        '''
        if(start_date==None):
            start_date = "2000-01-01"
        if(end_date==None):
            end_date = datetime.date.today().strftime("%Y-%m-%d")
        self.log.log("Log : try to get code {} data from  {} to {}".format(code,start_date,end_date),self.name)
        data_table =  "{}_price".format(self.rd.get_normalize_code(code))
        self.update_stock_data_start_end(code ,start_date,end_date)
        return self.__get_local_stock_data_start_end(code,start_date,end_date)
        
    def  create_stock_data_table(self,table):
        self.ld.create_table(table,{
            "date" :"date primary key",
            "open" :"double(10,2)",
            "close" :"double(10,2)",
            "low":"double(10,2)",
            "high":"double(10,2)",
            "volume" :"BIGINT",
            "money"   :"double(13,2)",
            "factor" :"double(10,2)",
            "high_limit" :"double(10,2)",
            "low_limit" :"double(10,2)",
            "avg"      :"double(10,2)",
            "pre_close":"double(10,2)",
            "paused" : "tinyint",
            "open_interest" :"double(10,2)"
        })
    def __get_remote_stock_data_start_end(self,code,start_date,end_date):
        self.log.log("Log : try to get remote code {} data from  {} to {}".format(code,start_date,end_date),self.name)
        return self.rd.get_stock_price(code,start_date,end_date)

    def __get_local_stock_data_start_end(self,code,start=None,end=None):
        where_limit =[] 
        if(start):
            where_limit.append(' date >= date("{}")'.format(start))
        if(end):
            where_limit.append('date<= date("{}")'.format(end))
        where =  ' and '.join(where_limit)
        table =  "{}_price".format(code)
        return self.ld.select_data(table,order_by = "Date",where_limit = None if len(where)<4 else where)

    def update_stock_data_start_end(self,code,start_date,end_date):
        data_table = "{}_price".format(code)
        if(not self.ld.has_table(data_table)):
            self.log.log("Warning : not find table {}, create it ".format(data_table))
            self.create_stock_data_table(data_table)    
        start_time = time.strptime(start_date,"%Y-%m-%d")
        start = datetime.date(start_time.tm_year,start_time.tm_mon,start_time.tm_mday)
        end = datetime.date.today()+datetime.timedelta(days=1)
        data =  self.__get_local_stock_data_start_end(code)
        if(len(data)>0):
            record_date_first =  data[0][0]
            record_date_last =  data[-1][0]
            self.log.log("Log :  local dataset find data from {} to {}".format(record_date_first,record_date_last),self.name)
            if(start<record_date_first):
                data =  self.__get_remote_stock_data_start_end(code,start.strftime("%Y-%m-%d"),record_date_first.strftime("%Y-%m-%d"))
                data=data.fillna(-1)
                if(len(data.index)>0):
                    data.insert(0,'date',data.index)
                    data['date'] = data['date'].apply(lambda x: '{}'.format(x.strftime("%Y%m%d")))
                    self.ld.insert_multi_data(data_table,data.to_numpy(), data.columns.to_list())
            if(end>record_date_last):
                data =  self.__get_remote_stock_data_start_end(code,record_date_last.strftime("%Y-%m-%d"),end.strftime("%Y-%m-%d"))
                data=data.fillna(-1)
                if(len(data.index)>0):
                    data.insert(0,'date',data.index)
                    data['date'] = data['date'].apply(lambda x:'{}'.format( x.strftime("%Y%m%d")))
                    self.ld.insert_multi_data(data_table,data.to_numpy(), data.columns.to_list())
        else:
            self.log.log("Log : no local data find for code {}".format(code),self.name)
                
            data =  self.__get_remote_stock_data_start_end(code,start.strftime("%Y-%m-%d"),end.strftime("%Y-%m-%d"))
            data=data.fillna(-1)
            if(len(data.index)>0):
                data.insert(0,'date',data.index)
                data['date'] = data['date'].apply(lambda x:'{}'.format( x.strftime("%Y%m%d")))
                self.ld.insert_multi_data(data_table,data.to_numpy(), data.columns.to_list())
    



if __name__=="__main__":
    k = MyDataset()
    k.init()
    p = k.get_stock_data_start_end('600435','2020-12-01','2021-01-01')
    print(p)
