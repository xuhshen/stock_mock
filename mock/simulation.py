'''
Created on 2019年3月5日

@author: 04yyl
'''
from pymongo import MongoClient
import datetime
import tushare as ts 
import os
import math
from pytdx.hq import TdxHq_API

class MongoDB(object):
    def __init__(self,ip="stock_mongo", #mongo db 数据库docker 容器名
                     port=27017, 
                     user_name=None, 
                     pwd=None,
                     authdb=None):
        self.__server_ip = ip
        self.__server_port = port
        self.__user_name = user_name
        self.__pwd = pwd
        self.__authdb = authdb
        self.client = None
        self.trade_day = True
        self.TDX_IP_SETS = ['119.147.164.60','218.75.126.9', '115.238.90.165',
                 '124.160.88.183', '60.12.136.250', '218.108.98.244', '218.108.47.69',
                 '14.17.75.71', '180.153.39.51']
        self.api = TdxHq_API(heartbeat=True)
        self.today = None
        self.accounts = []
        self.db = "stock_mock"   #数据库
        self.account_collection = "account"  #保存各个账户的当前资金信息
        self.account_his_collection = "account_history" #保存每个账户的历史净值信息
        self.prefix = "holdlist_"
        self.accounts = []
        self.stocks = []
            
    def connect(self):
        '''建立数据库的连接
        '''
        _db_session = MongoClient(self.__server_ip, self.__server_port)
        if  self.__user_name:        
            eval("_db_session.{}".format(self.__authdb)).authenticate(self.__user_name,self.__pwd)      
        
        self.client = _db_session

    def connect_market(self):
        for ip in self.TDX_IP_SETS:
            try:
                if self.api.connect(ip, 7709):
                    return 
            except:
                pass
    
    def disconnect(self):
        '''断开数据库连接        
        '''
        self._db_session.close()
        return True
    
    def _dbclient(self,db):
        '''返回某个特定数据库的对象
        '''
        return eval("self.client.{}".format(db))
    
    def handle_ex_right(self):
        '''处理持仓股票除权价格和数量
        ''' 
        func = lambda x: 0 if not x else x
        today = datetime.datetime.today().date().day
        year = datetime.datetime.today().date().year
        month = datetime.datetime.today().date().month
        
        for stock in self.stocks:
            mk = self._select_market_code(stock)
            cqcx = self.api.get_xdxr_info(mk,stock)[::-1]
            dct = {"fenhong":0,'peigu':0, 'peigujia':0,"songzhuangu":0}
            iscq = False
            for i in cqcx:
                if i["day"] != today or i["month"] != month or i["year"] != year:
                    break
                else:
                    iscq = True
                    dct["fenhong"] += func(i["fenhong"])
                    dct["peigu"] += func(i["peigu"])
                    dct["peigujia"] += func(i["peigujia"])
                    dct["songzhuangu"] += func(i["songzhuangu"])
            
            if iscq: #发生除权除息
                rst = self.api.get_security_bars(4,mk,stock,0,2)
                if rst[0]["day"]!=today or i["month"] != month or i["year"] != year:
                    close = rst[0]["close"]
                else:
                    close = rst[1]["close"] 
                preclose = (close*10-dct["fenhong"]+dct["peigu"]*dct['peigujia'])/(10+dct['peigu']+dct['songzhuangu'])
                rate = close/preclose
                for account in self.accounts:
                    filt = {"code":stock,"cx_date":{"$ne":self.today}}
                    dt = {"$mul": {"cost":1/rate, "number":rate},"$set":{"cx_date":self.today}}
                    self._dbclient(self.db)[self.prefix+account].update_one(filt,dt)
    
    def set_accounts(self):
        
        self.accounts = [i["account"] for i in  self._dbclient(self.db)[self.account_collection].find()]
    
    def set_stocks(self):
        rst = []
        for account in self.accounts:
            rst.extend([i["code"] for i in self._dbclient(self.db)[self.prefix+account].find({"number":{"$gt":0}},{"_id":0,"code":1})])
        self.stocks = set(rst)
    
    def initial(self):
        '''每天初始化状态，连接行情数据源，更新除权信息
        '''
        self.today = datetime.datetime.today().date().strftime('%Y-%m-%d')
        df = ts.trade_cal()
        self.trade_day = df[(df["calendarDate"]==self.today)].isOpen.values[0]
        if self.trade_day: #交易日，连接数据库，连接行情源，处理除权除息
            self.connect()
            self.connect_market()
            self.set_accounts()
            self.set_stocks()
            self.handle_ex_right() 

    def _select_market_code(self,code):
        code = str(code)
        if code[0] in ['5','6','9'] or code[:3] in ["009","126","110","201","202","203","204"]:
            return 1
        return 0 
    
    def updateaccount(self,account="test"):
        '''更新账户净值，添加一条净值记录
        '''
        hold_collection = self.prefix + account
        tm = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        plimit = 80
        filt = {'number':{'$gt':0}}
        stocks = [(self._select_market_code(i["code"]),i["code"]) for i in self._dbclient(self.db)[hold_collection].find(filt)]
        rst = {}
        for i in range(math.ceil(len(stocks)/plimit)):
            ss = stocks[i*plimit:(i+1)*plimit]
            for item in self.api.get_security_quotes(ss):
                rst[item["code"]] =  item["price"]
        
        if len(rst)>0:
            bulk = self._dbclient(self.db)[hold_collection].initialize_ordered_bulk_op()
            for code,price in rst.items():
                d = {"price":price,"update_datetime":tm}
                bulk.find({"code":code}).upsert().update({"$set":d})
            bulk.execute()
        holdvalue = self._dbclient(self.db)[hold_collection].aggregate([{ "$group": { "_id": None, 
                                                                                  "total": { "$sum": { "$multiply": [ "$price", "$number"]}}}}])
        holdvalue = [i for i in holdvalue][0]["total"]
        rst = self._dbclient(self.db)[self.account_collection].find({"account":account},{"_id":0})[0]
        rst["total"] = rst["rest"] + holdvalue
        rst["hold"] = holdvalue
        #更新账户当前值
        self._dbclient(self.db)[self.account_collection].update_one({"account":account},{"$set":rst})
        #更新账户历史记录值
        self._dbclient(self.db)[self.account_his_collection].update_one({"account":account,"date":self.today},{"$set":rst},upsert=True)
    
    def run(self):
        if not self.trade_day: #交易日，更新账户信息
            return 
        for account in self.accounts:
            self.updateaccount(account)
    
    
if __name__ == '__main__':
    from apscheduler.schedulers.blocking import BlockingScheduler
    ip = os.environ.get('IP',"192.168.0.106")
    mdb = MongoDB(ip=ip)
    mdb.initial()
    
    sched = BlockingScheduler()
    sched.add_job(mdb.initial,'cron', day_of_week='0-4', hour='9',minute='10',misfire_grace_time=60)
    sched.add_job(mdb.run,'cron', day_of_week='0-4', hour='9',minute='30-59/10',misfire_grace_time=60)
    sched.add_job(mdb.run,'cron', day_of_week='0-4', hour='10,11,13,14',minute='*/10',misfire_grace_time=60)
    sched.add_job(mdb.run,'cron', day_of_week='0-4', hour='15',minute='1',misfire_grace_time=60)
    
    sched.add_job(mdb.disconnect,'cron', day_of_week='0-4', hour='15',minute='5',misfire_grace_time=60)
     
    sched.start()
    





