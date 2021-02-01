from tool.mylog import MyLog
from remote_data.Remote_Data_Imp import remote_dataset
from local_data.Local_Data_Imp import LocalDataset

import jqdatasdk as jqs

import datetime

import time
import csv

class MyDataset():
    def __init__(self):
        self.log = MyLog.get_instance()
        self.ld =  LocalDataset()
        self.rd =  remote_dataset()
        self.name = "My Dataset"
        self.index_name_list = []
        self.stock_price_prop={
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
            "open_interest" :"double(10,2)",
        }

    def init(self):
        self.ld.init_local_data_set(host = "cdb-mqzvz536.bj.tencentcdb.com" ,port =10146 , user="root" , database="stock_test")
        self.rd.init()
        with open(".//index_name.csv",'r',encoding='GBK') as f:
            csvr= csv.reader(f)
            for i in csvr:
                self.index_name_list.append(i)
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
        self.update_stock_data_start_end(code ,start_date,end_date,self.__table_name_stock_price(code),
            self.stock_price_prop,
            self.__get_local_stock_price_data_start_end,
            self.__get_remote_stock_price_data_start_end
        )
        data1= self.__get_local_stock_price_data_start_end(code,start_date,end_date)
        return data1
        
    def __create_stock_data_table(self,table,prop):
        self.ld.create_table(table,prop)

    def __get_remote_stock_price_data_start_end(self,code,start_date,end_date):
        self.log.log("Log : try to get remote code {} data from  {} to {}".format(code,start_date,end_date),self.name)
        return self.rd.get_stock_price(code,start_date,end_date)

    def __get_local_stock_price_data_start_end(self,code,start=None,end=None):
        where_limit =[] 
        if(start):
            where_limit.append(' date >= date("{}")'.format(start))
        if(end):
            where_limit.append('date<= date("{}")'.format(end))
        where =  ' and '.join(where_limit)
        table =  self.__table_name_stock_price(self.rd.get_normalize_code(code))
        return self.ld.select_data(table,order_by = "Date",where_limit = None if len(where)<4 else where)

    def __table_name_stock_price(self,code):
        code = self.rd.get_normalize_code(code)
        return "{}_price".format(code).replace('.','_')

    def update_stock_data_start_end(self,code,start_date,end_date,data_table,create_prop,func_local,func_remote):
        '''
            template for get date list stock info
            code : stock code
            start_date end_date  : should be '%Y-%m-%d'
            data_table : save data table name in local
            create_prop: dict used for create table , index should always be same with name for remote columns
            func_local  func_remote : get info function from local and remote 
                                    param should be code start end

        '''
        if(not self.ld.has_table(data_table)):
            self.log.log("Warning : not find table {}, create it ".format(data_table),self.name)
            self.__create_stock_data_table(data_table,create_prop)    
        start_time = time.strptime(start_date,"%Y-%m-%d")
        start = datetime.date(start_time.tm_year,start_time.tm_mon,start_time.tm_mday)
        end = datetime.date.today()+datetime.timedelta(days=1)
        data =  func_local(code)
        if(len(data)>0):
            record_date_first =  data[0][0]
            record_date_last =  data[-1][0]
            self.log.log("Log :  local dataset find data from {} to {}".format(record_date_first,record_date_last),self.name)
            if(start<record_date_first):
                if(record_date_first-start == datetime.timedelta(1) and record_date_last.weekday()==1):
                    pass
                elif (record_date_first -start  == datetime.timedelta(2) and  record_date_last.weekday()==1):
                    pass
                else:
                    data =  func_remote(code,start.strftime("%Y-%m-%d"),record_date_first.strftime("%Y-%m-%d"))
                    data=data.fillna(-1)
                    if(len(data.index)>0):
                        data.insert(0,'date',data.index)
                        data['date'] = data['date'].apply(lambda x: '{}'.format(x.strftime("%Y%m%d")))
                        self.ld.insert_multi_data(data_table,data.to_numpy(), data.columns.to_list())
            if(end>record_date_last):
                if (end-record_date_last== datetime.timedelta(1) and end.weekday() == 5):
                    pass
                elif (end-record_date_last == datetime.timedelta(2) and end.weekday() == 5):
                    pass
                else:
                    data =  func_remote(code,record_date_last.strftime("%Y-%m-%d"),end.strftime("%Y-%m-%d"))
                    data=data.fillna(-1)
                    if(len(data.index)>0):
                        data.insert(0,'date',data.index)
                        data['date'] = data['date'].apply(lambda x:'{}'.format( x.strftime("%Y%m%d")))
                        self.ld.insert_multi_data(data_table,data.to_numpy(), data.columns.to_list())
        else:
            self.log.log("Log : no local data find for code {}".format(code),self.name)
            data =  func_remote(code,start.strftime("%Y-%m-%d"),end.strftime("%Y-%m-%d"))
            data=data.fillna(-1)
            if(len(data.index)>0):
                data.insert(0,'date',data.index)
                data['date'] = data['date'].apply(lambda x:'{}'.format( x.strftime("%Y%m%d")))
                self.ld.insert_multi_data(data_table,data.to_numpy(), data.columns.to_list())


    def get_multi_stock_data_start_end(self,code_list,start=None,end=None):
        data_dict = {}
        for code in code_list:
            data =  self.get_stock_data_start_end(code,start,end)
            data_dict[code] = data
        return data_dict

    #finish stock data part

    #start index weight part
    def show_index(self,name=None):
        for i in self.index_name_list:
            if(name !=None and name not in i[1]):
                continue
            print(i)

    def get_index_stocks(self,indexi,date=None):
        if(date==None):
            date =  datetime.date.today().strftime('%Y-%m-01')
        tablename  = '{}_index_weight_{}'.format(index,date).replace('.','')
        if(not self.ld.has_table(tablename)):
            self.create_index_weight_table(index,date)

    def create_index_weight_table(self,index,date):
        tablename  = '{}_index_weight_{}'.format(index,date).replace('.','')
        pass

if __name__=="__main__":
    k = MyDataset()
    k.init()
    lis = jqs.get_index_stocks('000300.XSHG')
    #k.show_index()
    #print("==delim==")
    #k.show_index('成长指数')
    #p = k.get_stock_data_start_end('600435','2020-11-01','2021-01-01')
    p = k.get_multi_stock_data_start_end(lis,'2018-01-01','2021-01-31')
    print(p)
