'''
Created on 2018年9月3日

@author: 04yyl
'''
'''商品组合交易模拟 15min 交易周期
'''
from pytdx.hq import TdxHq_API
from cfg import logger,STOCK_IP_SETS
from trade import trade
import os
import datetime
import tushare as ts

class SP(object):
    def __init__(self,userid="account4",number=2,mock=True):
        ''' 根据实际账户资金，以股指1手为基本对冲单位进行配置。
        '''
        self.products = {
                        '000905': {'args': (3, 6, 65), 'stocklst': {"510500":0}}
                         }
        self.datatype = 0
        self.userid = userid
        self.api = TdxHq_API(heartbeat=True)
        self.trader = None
        self.trading = False
        self.mock = mock
        self.permoney = 200 #指数一个点对应的金额
        self.number  = number #指数的倍数
        
        self.TDX_IP_SETS = STOCK_IP_SETS
        
    
    def connect(self):
        for ip in self.TDX_IP_SETS:
            try:
                if self.api.connect(ip, 7709):
                    return 
            except:
                pass
    
    def getdata(self,product,market=1,number=10000,pn=400):
        data = []
        for i in range(int(number/pn)+1):
            temp = self.api.get_index_bars(self.datatype, market, product, (int(number/pn)-i)*pn,pn)
            
            if not temp or len(temp)<pn:
                for _ in range(2):
                    temp = self.api.get_index_bars(self.datatype, market, product, (int(number/pn)-i)*pn,pn)
                    if not temp or len(temp)<pn:
                        logger.info("record not reach the limit!")
                    else:
                        break    
            data += temp
        df = self.api.to_df(data)[["open","close","high","low","datetime"]]
        df.set_index("datetime",inplace=True,drop=False)
        return df
    
    def disconnect(self):
        if not self.istradeday:
            return 
        
        self.api.disconnect()
    
    def set_number(self):
        '''设置日内交易的单次手数
        '''
        price = self.api.get_security_quotes([ (1, '510500')])[0]["last_close"]
        index_price = self.api.get_security_quotes([ (1, '000905')])[0]["last_close"]
        
        self.products["000905"]["stocklst"]["510500"] = int(index_price*self.number*self.permoney/price/100)*100
        

    def judgetradeday(self,):
        today = datetime.datetime.today().date().strftime('%Y-%m-%d')
        df = ts.trade_cal()
        return df[(df["calendarDate"]==today)].isOpen.values[0]

    @property
    def istradeday(self):
        if not hasattr(self, "_istradeday"):
            self._istradeday = self.judgetradeday()
        return self._istradeday
    
    def initial(self):
        '''每天初始化设置
        '''
        if not self.istradeday:
            self.trading = False
            return 
        self.trading = True
        logger.info("try to create connect... ")
        self.connect()
        self.trader = trade(UserID=self.userid,api=self.api,mock=self.mock)
        logger.info("connect successful!")
        
        self.set_number() #设置手数
        print(self.products)
        logger.info("initial account info finished")

    def handledata(self,df,args=[]):
        df.loc[:,"number"] = range(df.shape[0]) 
        s,m,l = args
        for i in args:#5 15 60 D
            key = str(5*i)
            df.loc[:,key+"high"] = df["high"].rolling(i).max()
            df.loc[:,key+"low"] = df["low"].rolling(i).min()
            df.loc[:,key+"atr"] = (df[key+"high"]-df[key+"low"]).rolling(10*i).mean()
            df.loc[:,key+"med"] = (df[key+"high"]+df[key+"low"])/2
            df.loc[:,key+"HH"] = df[key+"med"] + 1.5*df[key+"atr"]
            df.loc[:,key+"LL"] = df[key+"med"] - 1.5*df[key+"atr"]
            df.loc[:,key+'HHmax'] = df[key+'HH'].rolling(10*i).max()
            df.loc[:,key+'LLmin'] = df[key+'LL'].rolling(10*i).min()
            df.loc[df[key+'HH']>=df[key+'HHmax'],key+'hmark'] = df["number"]
            df.loc[df[key+'LL']<=df[key+'LLmin'],key+'lmark'] = df["number"]
            df[key+'hmark'].fillna(method="ffill",inplace=True)
            df[key+'hmark'].fillna(0,inplace=True)
            
            df[key+'lmark'].fillna(method="ffill",inplace=True)
            df[key+'lmark'].fillna(0,inplace=True)
            
            df.loc[:,key+'UP'] = df[key+'hmark'] >= df[key+'lmark']
#             debuginfo.append({key+'hmark':df.iloc[-1][key+'hmark'],key+'lmark':df.iloc[-1][key+'lmark']})
            
        df.fillna(method="ffill",inplace=True)
        df.dropna(inplace=True)
#         logger.info("trademessage:{}".format(debuginfo))
        result = (df.iloc[-1][str(5*l)+"UP"] >0)&(df.iloc[-1][str(5*s)+"UP"]>0)    
        result |= (df.iloc[-1][str(5*l)+"UP"]>0)&(df.iloc[-1][str(5*m)+"UP"]>0) 
        result |= (df.iloc[-1][str(5*l)+"UP"]<=0)&(df.iloc[-1][str(5*m)+"UP"]>0)&(df.iloc[-1][str(5*s)+"UP"]>0)    
        return result
    
    def sync(self,idx,director=True):
        stocks = self.products[idx]["stocklst"]
        for stock,number in stocks.items():
            if not director: number = number #空信号,清仓
            else: number = 3*number
            
            #判断现有持仓
#             h_number = self.hd_df.ix[stock]["参考持股"]
            try:
                h_number = self.hd_df.ix[stock]["参考持股"]
                 
            except:
                h_number = 0
        
            #补仓差
            cangcha = int((number-h_number)/100)*100
            
            if h_number>0 and abs(cangcha)/number<0.05: #如果有持仓，同时仓差小于5% 不进行更改，为了处理频繁加减仓达到问题
                return 
                
            if cangcha>0:
                logger.info("buy code:{}, number:{}".format(stock,number-h_number))
                self.buy(stock,cangcha)
            elif cangcha<0:
                logger.info("sell code:{}, number:{}".format(stock,h_number-number))
                self.sell(stock,-cangcha)
    
    def buy(self,stock,number):
        self.trader.buy(stock, number)
    
    def sell(self,stock,number):
        self.trader.sell(stock, number)
    
    def check_position(self,status):
        '''检查仓位情况
        '''
        self.handleposition()
        handlelist = []
        
        for ins,v in status.items():
            if ins not in self.g_df.index :
                handlelist.append(ins)
            elif self.g_df.ix[ins]["Position"] != v["number"]:
                handlelist.append(ins)
            
        return handlelist
    
    def handleposition(self):
        '''计算多仓，空仓，以及昨仓和今仓
        '''
        self.trader.cancelorder() #先尝试撤单
        _,holdlists = self.trader.position()
        self.hd_df = holdlists
        if holdlists.shape[0]>0:
            self.hd_df.set_index("证券代码",inplace=True)
        return self.hd_df
    
    def run(self):
        
        if not self.trading:
            return
        
        self.handleposition()
        
        rst = {}
        for idx in list(self.products.keys()):
            director = self.handledata(self.getdata(idx,market=1),self.products[idx]["args"]) #用指数出信号
            logger.info("trademessage: block:{}, director:{}".format(idx,director))
            self.sync(idx,director)
            rst[idx] = {"up":director,"number":self.products[idx]["stocklst"],"product":idx}
        
        logger.info("lastest position status:{}".format(rst))
            

if __name__ == '__main__':
    from apscheduler.schedulers.blocking import BlockingScheduler
    account = os.environ.get('ACCOUNT',"stock_mock_acc2")
    number = float(os.environ.get('NUMBER',"2"))
    mock = os.environ.get('MOCK',True)
    if mock == "False":mock = False

    s = SP(userid=account,number=number,mock=mock)
    s.initial()
#     s.run()
    sched = BlockingScheduler()
    sched.add_job(s.initial,'cron', day_of_week='0-4', hour='9',minute='25',misfire_grace_time=60)
      
    sched.add_job(s.run,'cron', day_of_week='0-4', hour='9',minute='44,49,54,59',misfire_grace_time=60)
    sched.add_job(s.run,'cron', day_of_week='0-4', hour='11',minute='4,9,14,19,24,29',misfire_grace_time=60)
    sched.add_job(s.run,'cron', day_of_week='0-4', hour='10,13,14',minute='4,9,14,19,24,29,34,39,44,49,54,59',misfire_grace_time=60)
      
    sched.add_job(s.disconnect,'cron', day_of_week='0-4', hour='15',minute='15',misfire_grace_time=60)
       
    sched.start()
    
    
    
    
