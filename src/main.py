'''
Created on 2018年9月3日

@author: 04yyl
'''
'''商品组合交易模拟 15min 交易周期
'''
from pytdx.hq import TdxHq_API
from cfg import logger,FILE_INCON,FILE_TDXHY,FILE_TDXZS,HY_WEIGHT,STOCK_IP_SETS
import gevent
from gevent import monkey;monkey.patch_all()
from trade import trade
import os
import re
import numpy as np
import datetime
import tushare as ts

class SP(object):
    def __init__(self,userid="account4",rate=1.2,products="1",limit=2,total=1000000,mock=True,server="http://192.168.118.1:65000"):
        if products == "1":
            self.products = {'880414': {'args': (3, 12, 90), 'stocklst': {}}, '880456': {'args': (6, 28, 40), 'stocklst': {}}, 
                             '880476': {'args': (5, 20, 90), 'stocklst': {}}, '880440': {'args': (4, 8, 85),  'stocklst': {}}, 
                             '880424': {'args': (4, 16, 110),'stocklst': {}}, '880448': {'args': (3, 14, 60), 'stocklst': {}}, 
                             '880454': {'args': (4, 8, 85),  'stocklst': {}}, '880493': {'args': (3, 24, 55), 'stocklst': {}}, 
                             '880344': {'args': (4, 4, 30),  'stocklst': {}}, '880301': {'args': (6, 20, 30), 'stocklst': {}}, 
                             '880464': {'args': (4, 14, 50), 'stocklst': {}}, '880459': {'args': (3, 16, 80), 'stocklst': {}}, 
                             '880380': {'args': (5, 22, 70), 'stocklst': {}}, '880472': {'args': (3, 32, 170),'stocklst': {}}, 
                             '880421': {'args': (3, 16, 45), 'stocklst': {}}, '880471': {'args': (4, 12, 40), 'stocklst': {}}, 
                             '880453': {'args': (6, 14, 30), 'stocklst': {}}, '880350': {'args': (4, 26, 30), 'stocklst': {}}, 
                             '880447': {'args': (5, 20, 170),'stocklst': {}}, '880351': {'args': (3, 6, 65),  'stocklst': {}}, 
                             '880390': {'args': (3, 14, 65), 'stocklst': {}}, '880406': {'args': (4, 16, 50), 'stocklst': {}}, 
                             '880305': {'args': (3, 14, 95), 'stocklst': {}}, '880492': {'args': (3, 30, 105),'stocklst': {}}, 
                             '880387': {'args': (5, 20, 70), 'stocklst': {}}, '880418': {'args': (4, 16, 100),'stocklst': {}}, 
                             '880367': {'args': (5, 14, 110),'stocklst': {}}, '880398': {'args': (5, 26, 100),'stocklst': {}}, 
                             '880437': {'args': (4, 8, 85),  'stocklst': {}}, '880474': {'args': (4, 24, 45), 'stocklst': {}}, 
                             '880324': {'args': (6, 22, 75), 'stocklst': {}}, '880335': {'args': (3, 12, 75), 'stocklst': {}}, 
                             '880372': {'args': (6, 20, 105),'stocklst': {}}, '880491': {'args': (4, 16, 20), 'stocklst': {}}, 
                             '880490': {'args': (3, 26, 110),'stocklst': {}}, '880431': {'args': (7, 12, 40), 'stocklst': {}}, 
                             '880432': {'args': (3, 10, 110),'stocklst': {}}, '880318': {'args': (4, 20, 90), 'stocklst': {}}, 
                             '880497': {'args': (3, 8, 40),  'stocklst': {}}, '880494': {'args': (3, 30, 110),'stocklst': {}}, 
                             '880400': {'args': (3, 10, 90), 'stocklst': {}}, '880360': {'args': (3, 16, 70), 'stocklst': {}}, 
                             '880310': {'args': (3, 12, 20), 'stocklst': {}}, '880423': {'args': (5, 16, 30), 'stocklst': {}}, 
                             '880430': {'args': (7, 12, 25), 'stocklst': {}}, '880422': {'args': (6, 8, 35),  'stocklst': {}}, 
                             '880465': {'args': (4, 12, 95), 'stocklst': {}}, '880482': {'args': (3, 20, 180),'stocklst': {}}, 
                             '880355': {'args': (3, 18, 45), 'stocklst': {}}, '880473': {'args': (3, 6, 40),  'stocklst': {}}, 
                             '880446': {'args': (3, 8, 45),  'stocklst': {}}, '880452': {'args': (3, 32, 90), 'stocklst': {}}, 
                             '880455': {'args': (5, 6, 20),  'stocklst': {}}, '880399': {'args': (3, 8, 85),  'stocklst': {}}, 
                             '880330': {'args': (5, 12, 80), 'stocklst': {}}, '880489': {'args': (3, 18, 190),'stocklst': {}}}
        elif products == "2": #资金比较少是，选择配置部分行业
            self.products = {'880414': {'args': (3, 12, 90), 'stocklst': {}}, '880456': {'args': (6, 28, 40), 'stocklst': {}}, 
                            '880476': {'args': (5, 20, 90), 'stocklst': {}}, '880440': {'args': (4, 8, 85),  'stocklst': {}}, 
                            '880424': {'args': (4, 16, 110),'stocklst': {}}, '880448': {'args': (3, 14, 60), 'stocklst': {}}, 
                            '880454': {'args': (4, 8, 85),  'stocklst': {}}, '880493': {'args': (3, 24, 55), 'stocklst': {}}, 
                             '880344': {'args': (4, 4, 30),  'stocklst': {}}, '880301': {'args': (6, 20, 30), 'stocklst': {}}, 
                             '880464': {'args': (4, 14, 50), 'stocklst': {}}, '880459': {'args': (3, 16, 80), 'stocklst': {}}, 
                             '880380': {'args': (5, 22, 70), 'stocklst': {}}, '880472': {'args': (3, 32, 170),'stocklst': {}}, 
                             '880421': {'args': (3, 16, 45), 'stocklst': {}}, '880471': {'args': (4, 12, 40), 'stocklst': {}}, 
                             '880453': {'args': (6, 14, 30), 'stocklst': {}}, '880350': {'args': (4, 26, 30), 'stocklst': {}}, 
                             '880447': {'args': (5, 20, 170),'stocklst': {}}, '880351': {'args': (3, 6, 65),  'stocklst': {}}, 
                             '880390': {'args': (3, 14, 65), 'stocklst': {}}, '880406': {'args': (4, 16, 50), 'stocklst': {}}, 
                             '880305': {'args': (3, 14, 95), 'stocklst': {}}, '880492': {'args': (3, 30, 105),'stocklst': {}}, 
                             '880387': {'args': (5, 20, 70), 'stocklst': {}}, '880418': {'args': (4, 16, 100),'stocklst': {}},
                             }
        self.server = server
        self.datatype = 1
        self.userid = userid
        self.limit = limit
        self.api = TdxHq_API(heartbeat=True)
        self.trader = None
        self.trading = False
        self.mock = mock
        
        self.rate = rate #持仓杠杆率
        self.total = total #默认持仓资金
        self.TDX_IP_SETS = STOCK_IP_SETS
        
        self.file_incon = FILE_INCON
        self.file_tdxhy = FILE_TDXHY
        self.file_tdxzs = FILE_TDXZS
        
    
    def connect(self):
        for ip in self.TDX_IP_SETS:
            try:
                if self.api.connect(ip, 7709):
                    return 
            except:
                pass
    
    def getdata(self,product,market=1,number=5000,pn=400):
        data = []
        for i in range(int(number/pn)+1):
            temp = self.api.get_index_bars(self.datatype, market, product, (int(number/pn)-i)*pn,pn)
            
            if not temp or len(temp)<pn:
                self.connect()
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
        logger.info("[DISCONNECT]:statrt disconnect!!!!!")
        if self.istradeday:
            self.api.disconnect()
        logger.info("[DISCONNECT]:disconnect finished!!!!!")
    
    def updatetotal(self):
        '''更新总资金
        '''
        accountinfo,holdlists = self.trader.position()
        nhg = holdlists[holdlists[u"证券代码"].map(lambda x:x in ["SZRQ88","SHRQ88"])][u"最新市值"].sum()
        self.total = accountinfo.ix["总资产"]["人民币"]-nhg
        return self.total
    
    def set_permoney(self):
        '''单个品种资金上限
        '''
        self.permoney = self.total * self.rate /len(self.products)
        return self.permoney
    
    def _get_incon(self,):
        '''获取行业分类代码
        '''
        f= open(self.file_incon, "rb")
        data = f.read()
        strings = data.decode("gbk", 'ignore').rstrip("\x00").replace("\r\n","\n")
        data = strings.split("######") 
        rst = {}
        for hystr in data:
            key = re.findall(r'#.*',hystr)
            if key == ['#TDXNHY']:
                hylst = hystr.replace("#TDXNHY","").strip("\n").split("\n")
                for item in hylst:
                    k,v = item.split("|")
                    rst[k] = [v]
        return rst

    def _get_tdxhy(self,islocal=True):
        '''获取股票和行业对应列表
        '''
        if islocal:
            stocklist = HY_WEIGHT.keys()
        else:
            stocklist = list(ts.get_stock_basics().index)  #获取全市场股票代码
        
        rst = self._get_incon()
        f= open(self.file_tdxhy, "rb")
        data = f.read().decode("gbk", 'ignore').rstrip("\x00").replace("\r\n","\n").strip("\n").split("\n")
                
        for i in data:
            _,code,tdxhy,_,_ = i.split("|")
            if tdxhy != "T00" and code in stocklist:
                rst[tdxhy].append(code)
        return rst

    def _get_tdxzs(self,islocal=True):
        '''生成通达性版块代码对应股票列表
        '''
        dct = {}
        rst = self._get_tdxhy(islocal=islocal)
        f= open(self.file_tdxzs, "rb")
        data = f.read().decode("gbk", 'ignore').rstrip("\x00").replace("\r\n","\n").strip("\n").split("\n")
        for i in data:
            name,code,_,_,_,hy = i.split("|")
            code = int(code)
            if 880301<=code and 880497>=code and hy in rst.keys() :
                k = hy[:5]
                if not dct.__contains__(k):
                    dct[k] = {"name":"","code":"","stocklist":[]}
                if k==hy: 
                    dct[k]["name"] = name
                    dct[k]["code"] = code
                dct[k]["stocklist"].extend(rst[hy][1:])
        return dct

    def get_tdxhy_list(self,islocal=True):
        '''获取通达信行业板块指数对应的股票列表
        '''
        return self._get_tdxzs(islocal)
    
    def get_weight(self,htlist={},islocal=True):
        '''获取行业板块个股权重，流动市值为权重系数
                   备注：回测是为方便处理，以最后一天的权重系数作为历史上的权重
        '''
        if islocal:
            self.weight = HY_WEIGHT
        else:
            if not htlist:
                htlist = self.get_tdxhy_list(islocal)
            tasks = []
            for v in htlist.values():
                tasks.append(gevent.spawn(self.get_latest_ltsz,v["stocklist"]))
            gevent.joinall(tasks)
            
        return self.weight
 
    def get_latest_ltsz(self,stocks=[]):
        '''获取最新流通市值,千万为单位，取整
        '''
        unit = 10000000
        for code in stocks:
            mk = self._select_market_code(code)
            print(mk,code)
            try:
                ltgb = self.api.get_finance_info(mk,code)["liutongguben"]
                price = self.api.get_security_bars(4,mk,code,0,1)[0]["close"]
                ltsz = int(ltgb*price/unit)
                self.weight[code] = ltsz
            except:
                print("*****",code)
        return 
    
    def set_instrument(self):
        '''设置交易股票
        '''
        func = lambda x :1 if x.startswith("6") else 0
        weight = self.get_weight()
        for v in self.get_tdxhy_list().values():
            code = str(v["code"])
            if not self.products.__contains__(code):continue
            
            stocks = [(i,weight[i]) for i in v["stocklist"]]
            limit_stocks = sorted(stocks,key=lambda x:x[1],reverse=True)[:self.limit]
            total = sum([i[1] for i  in limit_stocks])
            for i in limit_stocks:
                market = func(i[0])
                price = self.api.get_security_bars(4,market,i[0],0,1)[0]["close"]
                number = int(self.permoney*i[1]/total/price/100)*100
                self.products[code]["stocklst"][i[0]] = number
        return self.products

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
        logger.info("[INITIAL]:start initial !!!!!!")
        if not self.istradeday:
            self.trading = False
            return 
        self.trading = True
        logger.info("[INITIAL]:try to create connect... ")
        self.connect()
        self.trader = trade(UserID=self.userid,api=self.api,mock=self.mock,server=self.server)
        logger.info("[INITIAL]:connect successful!")
        
        logger.info("[INITIAL]:initial account info...")
        self.updatetotal() #更新账户总资金
        self.set_permoney() #设置单个品种资金上限
        logger.info("[INITIAL]:set per product money limit:{}".format(self.permoney))
        self.set_instrument() #设置交易股票和手数
        logger.info("[INITIAL]:set stock list succcessful !!!")
        logger.info("[INITIAL]:initial finished!!!!!!")

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
            if not director: number = 0 #空信号,清仓
            
            #判断现有持仓
            try:
                code = stock
                h_number = self.hd_df.ix[code]["证券数量"]
            except:
                h_number = 0
            logger.info("[RUN]:{},{},{}".format(stock,h_number,number))
        
            #补仓差
            cangcha = int((number-h_number)/100)*100
            
            if h_number>0 and abs(cangcha)/h_number<0.2: #如果有持仓，同时仓差小于10% 不进行更改，为了处理频繁加减仓达到问题
                continue 
                 
            if cangcha>0:
                logger.info("[RUN]:buy code:{}, number:{}".format(stock,number-h_number))
                self.buy(stock,cangcha)
            elif cangcha<0:
                couldsell = self.hd_df.ix[code]["可卖数量"]
                logger.info("[RUN]:sell code:{}, number:{},couldsell:{}".format(stock,h_number-number,couldsell))
                if couldsell >0:
                    self.sell(stock,min(-cangcha,couldsell))
#     
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
            self.hd_df.index = self.hd_df.index.astype(np.str).map(lambda x:x if len(x)>=6 else "0"*(6-len(x))+x)
            
        return self.hd_df
    
    def run(self):
        logger.info("[RUN]:start run !!!!!")
        if not self.trading:
            return
        
        self.handleposition()
        
        rst = {}
        for idx in list(self.products.keys()):
            director = self.handledata(self.getdata(idx,market=1),self.products[idx]["args"]) #用指数出信号
            logger.info("[RUN]:trademessage: block:{}, director:{}".format(idx,director))
            self.sync(idx,director)
            rst[idx] = {"up":director,"number":self.products[idx]["stocklst"],"product":idx}
        
        logger.info("[RUN]:lastest position status:{}".format(rst))
        logger.info("[RUN]:run finished !!!!!")    

if __name__ == '__main__':
    from apscheduler.schedulers.blocking import BlockingScheduler
    account = os.environ.get('ACCOUNT',"account2")
    rate = float(os.environ.get('RATE',"1"))
    products = os.environ.get('PRODUCTS',"2")
    server=os.environ.get('SERVER',"http://192.168.0.100:65000")
    
    mock = os.environ.get('MOCK',True)
    if mock == "False":mock = False

    s = SP(userid=account,rate=rate,products=products,mock=mock,server=server)
    try:
        s.initial()
    except:
        s.initial()
    sched = BlockingScheduler()
    sched.add_job(s.initial,'cron', day_of_week='0-4', hour='9',minute='25',misfire_grace_time=60)
      
    sched.add_job(s.run,'cron', day_of_week='0-4', hour='9',minute='44,59',misfire_grace_time=60)
    sched.add_job(s.run,'cron', day_of_week='0-4', hour='11',minute='14,29',misfire_grace_time=60)
    sched.add_job(s.run,'cron', day_of_week='0-4', hour='10,13,14',minute='14,29,44,56',misfire_grace_time=60)
      
    sched.add_job(s.disconnect,'cron', day_of_week='0-4', hour='15',minute='15',misfire_grace_time=60)
       
    sched.start()
    
    
    
    
