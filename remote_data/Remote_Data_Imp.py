import jqdatasdk as jqs
import sys
sys.path.append("..")
from tool.mylog  import MyLog as  Mylog

class remote_dataset():
    def __init__(self):
        self.log = Mylog.get_instance()
        self.name =  "Remote  Dataset"
        self.login = True

    def  init(self,username=None,password=None):
        if(username ==None):
            username = str(input('RD Username:'))
        if(password==None):
            password = str(input("RD Password : "))
        jqs.auth(username,password)
        if(jqs.is_auth()):
            self.log.log("Log : jq auth login" , self.name)
            self.login =  True
        else:
            self.log.log("ERROR : jq login fail",self.name)

    def get_normalize_code(self,code):
        return jqs.normalize_code(code)

    def  get_stock_list(self,start_date=None):
        # 返回全部股票信息列表
        data = None
        if(self.login):
            data =  jqs.get_all_securities(['stock'],date=start_date)
            self.log.log("Log : Get all stock list "+ "from date {}".format(start_date) if start_date!=None else "" , self.name )
        return data;

    def get_stock_index_weight(self,index_id,start_date=None):
        # 返回 指数成分股以及权重
        data = None
        stock_index = index_id
        #stock_index = self.get_normalize_code(index_id)
        if(self.login):
            data = jqs.get_index_weights(stock_index,date=start_date)
            self.log.log("Log : Get all stock {} weight ".format(stock_index) + "from date {}".format(start_date) if start_date != None else "",
                         self.name)

        return data

    def get_stock_price(self,code,start_date = None,end_date=None,frequency="daily",skip_paused=False,fq='none',count=None,panel=True,fill_paused=True):
        '''
        get stock price
        :param code:
        :param start_date:  count 不可共存
        :param end_date:  如果 end_date 只有日期, 则日内时间等同于 00:00:00, 所以返回的数据是不包括 end_date 这一天的.
        :param frequency: default is daily 现在支持'Xd','Xm', 'daily'(等同于'1d'), 'minute'(等同于'1m'), X是一个正整数, 分别表示X天和X分钟
        :param skip_paused:. 如果不跳过, 停牌时会使用停牌前的数据填充(具体请看SecurityUnitData的paused属性), 上市前或者退市后数据都为 nan, 但要注意:
            默认为 False
            当 skip_paused 是 True 时, 获取多个标的时需要将panel参数设置为False(panel结构需要索引对齐)
        :param fq: 复权 'pre': 前复权
                        None: 不复权, 返回实际价格 dafault
                       'post': 后复权
        :param count:
        :param panel:
        :param fill_paused: 对于停牌股票的价格处理，默认为True；True表示用pre_close价格填充；False 表示使用NAN填充停牌的股票价格。
        :return: price info
        fields: ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit','low_limit', 'avg', 'pre_close', 'paused', 'open_interest'], open_interest为期货持仓量

        '''
        fields= ['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit','low_limit', 'avg', 'pre_close', 'paused', 'open_interest']
        data = None
        stock_code = self.get_normalize_code(code)
        if(self.login):
            data = jqs.get_price(stock_code,start_date=start_date , end_date=end_date , frequency=frequency ,
                    fields=fields,skip_paused=skip_paused,fq=fq,count=count,panel=panel , fill_paused=fill_paused)
            self.log.log("Log : Get stock {} info ".format(stock_code) ,self.name)
        return data



    def get_market_inf(self,start_date= None,n=None,desc=False):
        data = None
        start_str=''
        n_str=''
        desc_str = ''
        if(self.login):
            cmd = jqs.query(jqs.finance.STK_EXCHANGE_TRADE_INFO)
            if(start_date!=None):
                cmd = cmd.filter(jqs.finance.STK_EXCHANGE_TRADE_INFO.date>=start_date)
                start_str='from {} '.format(start_date)
            if(n!=None):
                cmd = cmd.limit(n)
                n_str =  'limit {} '.format(n)
            if(desc):
                cmd = cmd.order_by(jqs.finance.STK_EXCHANGE_TRADE_INFO.date.desc())
                desc_str = 'order desc'
            data = jqs.finance.run_query(cmd)
            self.log.log("Log : Get market info {} {} {}".format(start_str,n_str,desc_str), self.name)
        return data



    def get_trade_days(self,start_date= None,end_date=None,count=None):
        data = None
        if(start_date!=None and count != None):
            self.print_line("Error  : trade day info can't work with start_date and count")
        if(self.login):
            data =  jqs.get_trade_days(start_date=start_date,end_date=end_date,count=count)
            self.log.log("Log : Get all trade days " + "from date {}".format(start_date) if start_date != None else ""
                         +"to date {}".format(end_date) if end_date != None else ""
                         +"count {}".format(count) if count != None else "",
                         self.name)
        return data


if __name__ =="__main__":
    rd = remote_dataset()
    rd.init()
    stock_list = rd.get_stock_list(start_date='2021-01-01')
    print(stock_list)
    stock_weight = rd.get_stock_index_weight('000001.XSHG','2018-05-03')
    print(stock_weight)
    price600435 =rd.get_stock_price('600435',end_date='2021-01-23',count=10)
    print(price600435)
    market_info = rd.get_market_inf(start_date='2021-01-01',desc=True)
    print(market_info)
    trade_days = rd.get_trade_days(end_date='2021-01-23',count=10)
    print(trade_days)
