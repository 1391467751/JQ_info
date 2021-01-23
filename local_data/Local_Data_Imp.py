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
        self.table_list =[]


    def init_local_data_set(self,host=None,user=None,password=None,database = None ,port = None,charset='utf8'):
        if(self.db!=None):
            self.cursor.close()
            self.db.close()
        if(host ==None):
            host = str(input("ld host :"))
        if(user == None):
            user =  str(input("ld user :"))
        if(password == None):
            password = str(input("ld password :"))
        if(database == None):
            database = str(input("ld database :"))
        if(port == None):
            port = int(input("ld port : "))
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
        table_list = self.execute("show tables")
        self.table_list  = [i[0] for i in table_list]

    def has_table(self,table_name):
        return table_name in self.table_list


    def execute(self,command):
        if(self.db==None):
            return None
        self.cursor.execute(command)
        self.Log.log("Log : execute command : {}".format(command),self.name)
        return self.cursor.fetchall()

    def create_table(self,table_name, prop_dict):
        if(table_name not in self.table_list):
            cmd =  "Create table {} (".format(table_name)+','.join(["{} {}".format(i,prop_dict[i]) for i in prop_dict])+");"
            self.execute(cmd)
            self.table_list.append(table_name)




    def insert_multi_data(self,table_name,values , columns = None):
        columns_str = ""
        if(columns!=None):
            columns_str = '( '+ ','.join(columns)+')'
        value_str = ''.join(['(' , '),('.join([','.join(['{}'.format(j) for j in values[i] ]) for i in values] )  , ')'])
        sql = 'insert into {} {} values {}'.format(table_name , columns_str,value_str)
        return self.execute(sql)

    def select_data(self,table_name , columns= None ,order_by = None , where_limit = None):
        if(table_name not in self.table_list):
            return None
        columns_str ='*'
        order_str=" "
        where_str = " "
        if(columns!=None):
            columns_str = ','.join(columns)
        if(order_by!=None):
            order_str = " order by {} ".format(order_by)
        if(where_limit != None):
            where_str = "Where {}".format(where_limit)

        cmd = " ".join(['select' ,columns_str, "from {}".format(table_name),where_str,order_by])
        return self.execute(cmd)


    def __del__(self):
        if(self.db!=None):
            self.cursor.close()
            self.db.close()

if __name__=="__main__":
    LD = LocalDataset()    
    LD.init_local_data_set(host = "cdb-mqzvz536.bj.tencentcdb.com" ,port =10146 , user="root" , database="stock_test")
