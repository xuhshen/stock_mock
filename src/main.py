'''
Created on 2018年9月3日

@author: 04yyl
'''
'''商品组合交易模拟 15min 交易周期
'''
from pytdx.hq import TdxHq_API
from cfg import logger,FILE_INCON,FILE_TDXHY,FILE_TDXZS,HY_WEIGHT
import gevent
from gevent import monkey;monkey.patch_all()
from trade import trade
import os
import re
import datetime
import tushare as ts

class SP(object):
    def __init__(self,userid="125733",rate=1.2,products="1",limit=2,total=1000000):
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
        self.datatype = 1
        self.userid = userid
        self.limit = limit
        self.api = TdxHq_API(heartbeat=True)
        self.trader = None
        self.trading = False
        
        self.rate = rate #持仓杠杆率
        self.total = total #默认持仓资金
        self.TDX_IP_SETS = ['119.147.164.60','218.75.126.9', '115.238.90.165',
                 '124.160.88.183', '60.12.136.250', '218.108.98.244', '218.108.47.69',
                 '14.17.75.71', '180.153.39.51']
        
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
    
    def updatetotal(self):
        '''更新总资金
        '''
#         accountinfo,_ = self.trader.position()
#         self.total = accountinfo["总资产"]["人民币"]
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
            stocks = [(i,weight[i]) for i in v["stocklist"]]
            limit_stocks = sorted(stocks,key=lambda x:x[1],reverse=True)[:self.limit]
            total = sum([i[1] for i  in limit_stocks])
            for i in limit_stocks:
                market = func(i[0])
                price = self.api.get_security_bars(4,market,i[0],0,1)[0]["close"]
                number = int(self.permoney*i[1]/total/price/100)
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
        if not self.istradeday:
            self.trading = False
            return 
        self.trading = True
        logger.info("try to create connect... ")
        self.connect()
        self.trader = trade(UserID=self.userid,api=self.api)
        logger.info("connect successful!")
        
        logger.info("initial account info...")
        self.updatetotal() #更新账户总资金
        self.set_permoney() #设置单个品种资金上限
        logger.info("set per product money limit:{}".format(self.permoney))
        self.set_instrument() #设置交易股票和手数
        logger.info("set product zhu li he yue succcessful !!!")

    def handledata(self,df,args=[]):
        df.loc[:,"number"] = range(df.shape[0]) 
        s,m,l = args
        debuginfo = []
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
            debuginfo.append({key+'hmark':df.iloc[-1][key+'hmark'],key+'lmark':df.iloc[-1][key+'lmark']})
            
        df.fillna(method="ffill",inplace=True)
        df.dropna(inplace=True)
        logger.info("trademessage:{}".format(debuginfo))
        result = df.iloc[-1][str(5*l)+"UP"] >0    
        result |= (df.iloc[-1][str(5*l)+"UP"]>0)&(df.iloc[-1][str(5*m)+"UP"]>0) 
        result |= (df.iloc[-1][str(5*l)+"UP"]<=0)&(df.iloc[-1][str(5*m)+"UP"]>0)&(df.iloc[-1][str(5*s)+"UP"]>0)    
        return result
    
    def sync(self,product,director=True):
        ins = self.products[product]["zlhy"]
        number = self.products[product]["number"]
        
        if ins in self.g_df.index:
            h_number = self.g_df.ix[ins]["newposition"]
            up_number = self.g_df.ix[ins]["yup"] + self.g_df.ix[ins]["tup"]
            down_number = self.g_df.ix[ins]["ydown"] + self.g_df.ix[ins]["tdown"]
            
            if director: #多单 2：空单   1：多单
                logger.info("holdnumber:{} ,limitnumber:{},up !".format(h_number,number))
                if down_number >0:
                    self.sell_close(ins)
                    
                if up_number < number:
                    self.buy_open(ins,number-up_number)
                elif h_number > number:
                    self.buy_close(ins,h_number-number)
            else: #空单
                logger.info("holdnumber:{} ,limitnumber:{},down!".format(h_number,number))
                if up_number>0:
                    self.buy_close(ins)
                
                if down_number<number:
                    self.sell_open(ins,number-down_number)
                elif -h_number>number:
                    self.sell_close(ins,-h_number-number)
        else:
            logger.info("InstrumentID not hold ! new open !")
            if director: #多单
                self.buy_open(ins,number)
            else:
                self.sell_open(ins,number)
    
    def close(self,ins):
        
        if self.g_df.ix[ins]["yup"] +self.g_df.ix[ins]["tup"] >0:
            self.buy_close(ins) 
        if self.g_df.ix[ins]["ydown"] +self.g_df.ix[ins]["tdown"] >0:
            self.sell_close(ins)
            
    def buy_open(self,ins,number):
        '''开多仓
        '''
        ins_b = bytes(ins, encoding = "utf8")
        self.trader.buy_open(ins_b,int(number),jump=0) 
    
    def buy_close(self,ins,number=0):
        '''平多仓
        '''
        y_num = int(self.g_df.ix[ins]["yup"])
        t_num = int(self.g_df.ix[ins]["tup"])
        ins_b = bytes(ins, encoding = "utf8")
        number = int(number)
        
        if number>0:
            if y_num>0 and y_num>number:
                self.trader.buy_close(ins_b,number,jump=0) 
            elif y_num>0 and y_num<number:
                self.trader.buy_close(ins_b,y_num,jump=0) 
                self.trader.buy_close(ins_b,number-y_num,today=True,jump=0)
            else:
                self.trader.buy_close(ins_b,number,today=True,jump=0)
        else:
            if y_num>0:
                self.trader.buy_close(ins_b,y_num,jump=0) 
            if t_num>0:
                self.trader.buy_close(ins_b,t_num,today=True,jump=0)
    
    def sell_open(self,ins,number):
        '''开空仓
        ''' 
        ins_b = bytes(ins, encoding = "utf8")
        self.trader.sell_open(ins_b,int(number),jump=0)
        
    def sell_close(self,ins,number=0):
        '''平空仓
        '''
        logger.info("sell close,number:{}".format(number))
        y_num = int(self.g_df.ix[ins]["ydown"])
        t_num = int(self.g_df.ix[ins]["tdown"])
        ins_b = bytes(ins, encoding = "utf8")
        number = int(number)
        
        if number>0:
            if y_num>0 and y_num>number:
                self.trader.sell_close(ins_b,number,jump=0) 
            elif y_num>0 and y_num<number:
                self.trader.sell_close(ins_b,y_num,jump=0) 
                self.trader.sell_close(ins_b,number-y_num,today=True,jump=0)
            else:
                self.trader.sell_close(ins_b,number,today=True,jump=0)
        else:
            if y_num>0:
                self.trader.sell_close(ins_b,y_num,jump=0) 
            if t_num>0:
                self.trader.sell_close(ins_b,t_num,today=True,jump=0)
    
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
        _,holdlists = self.trader.position()
        self.hd_df = holdlists[["证券代码","参考持股"]]
        self.hd_df.set_index("证券代码",inplace=True)
        return self.hd_df
    
    def run(self):
        
        if not self.trading:
            return
        
        self.handleposition()

        zllist = [v["zlhy"] for v in self.products.values()]
        rst = {}
        
        for ins,ins_t in list(self.monitorlist.items()):
            product = ins_t[:-4]
            if ins not in zllist and ins not in self.df_h.index:
                del self.monitorlist[ins]
            elif ins not in zllist and ins in self.df_h.index:
                logger.info("close not main ins:{}".format(ins))
                self.close(ins)
            else:
                market = self.products[product]["market"]
                director = self.handledata(self.getdata(ins_t[:-4]+"L9",market),self.products[product]["args"]) #用指数出信号
                self.sync(product,director)
                rst[ins] = {"up":director,"number":self.products[product]["number"],"product":product}
        
        for _ in range(3): #尝试最多检查三次持仓情况和理论持仓状态
            handlelist = self.check_position(rst)
            if len(handlelist) == 0:
                break
            for ins in handlelist:
                self.sync(rst[ins]["product"],rst[ins]["up"])
            
                
        logger.info("lastest position status:{}".format(rst))
            

if __name__ == '__main__':
    from apscheduler.schedulers.blocking import BlockingScheduler
    account = os.environ.get('ACCOUNT',"125733")
    rate = float(os.environ.get('RATE',3))
    products = os.environ.get('PRODUCTS',"1")
    
    s = SP(userid=account,rate=rate,products=products)
    s.initial()
#     
#     sched = BlockingScheduler()
#     sched.add_job(s.initial,'cron', day_of_week='0-4', hour='9,21',minute='1',misfire_grace_time=60)
#     sched.add_job(s.run,'cron', day_of_week='0-4', hour='0,9,10,14,21,22,23',minute='14,29,44,59',misfire_grace_time=60)
#     sched.add_job(s.run,'cron', day_of_week='0-4', hour='11',minute='14,29',misfire_grace_time=60)
#     sched.add_job(s.run,'cron', day_of_week='0-4', hour='13',minute='44,59',misfire_grace_time=60)
#     sched.add_job(s.disconnect,'cron', day_of_week='0-4', hour='15',minute='1',misfire_grace_time=60)
#     sched.add_job(s.disconnect,'cron', day_of_week='1-5', hour='1',minute='1',misfire_grace_time=60)
#     
#     sched.start()
    
    
    
    
