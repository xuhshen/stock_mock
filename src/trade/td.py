'''
Created on 2019年3月4日

@author: 04yyl
'''
import requests
import json
from requests.auth import HTTPBasicAuth
import pandas as pd
from pymongo import MongoClient 
import datetime

class trade(object):
    def __init__(self,UserID="xuhshen",api=None,server="http://192.168.0.100:5000",mock=True):
        self.api = api
        self.server = server
        self.account = UserID
        self.mock = mock
    
    @property    
    def token(self):
        user=self.account
        passwd=self.account
        r = requests.get(self.server+'/api/token', auth=HTTPBasicAuth(user, passwd))
        token = json.loads(r.text)["token"]
        return token
    
    def position(self):
        if self.mock:
            pass
        else:
            r=requests.get(self.server+'/positions', auth=HTTPBasicAuth(self.token, 'x'))
            data=json.loads(r.text)
            holdlists = pd.DataFrame(data["dataTable"]["rows"],columns=data["dataTable"]["columns"] )
            accountinfo = pd.DataFrame(data["subAccounts"])
        return accountinfo,holdlists
    
    def _select_market_code(self,code):
        code = str(code)
        if code[0] in ['5','6','9'] or code[:3] in ["009","126","110","201","202","203","204"]:
            return 1
        return 0 
    
    def get_latest_price(self,code):
        '''获取最新价格
        bid:买
        ask:卖
        '''
#         fields = ["bid1","bid_vol1",
#                   "bid2","bid_vol2",
#                   "bid3","bid_vol3",
#                   "bid4","bid_vol4",
#                   "bid5","bid_vol5",
#                   "ask1","ask_vol1",
#                   "ask2","ask_vol2",
#                   "ask3","ask_vol3",
#                   "ask4","ask_vol4",
#                   "ask5","ask_vol5",
#                   "last_close"]
        market = self._select_market_code(code)
#         api.get_security_quotes([(0, '000001'), (1, '600300')])
        rst = self.api.get_security_quotes(market, code)[0]
        
        return rst
    
    def set_buy_price(self,stock):
        market = self.get_latest_price(stock)
        buy_price = market["bid1"]
        return buy_price
    
    def set_sell_price(self,stock):
        market = self.get_latest_price(stock)
        sell_price = market["ask1"]
        return sell_price            
    
    def buy(self,stock,number):
        '''买入股票
        '''
        price = self.set_buy_price(stock)
        postdata={"action":0,"priceType":0,"price":price,"amount":number,"symbol":stock}
        self.order(postdata)    
        
    def sell(self,stock,number):
        price = self.set_sell_price(stock)
        postdata={"action":1,"priceType":0,"price":price,"amount":number,"symbol":stock}
        self.order(postdata)  

    def order(self,postdata):
        '''
        example:
            postdata={"action":3,# 0 买入 1 卖出 2 融资买入 3 融券卖出 4 买券还券 5 卖券还款 6 现券还券
           "priceType":0,
           "price":3.132,
           "amount":200, #必须是整型，不能是float，c_int转换貌似失效
           "symbol":"131810"}  #131810  204001
        '''
        if self.mock:
            pass
        else:
            r=requests.post(self.server+'/orders',json=postdata,auth=HTTPBasicAuth(self.token, 'x'))
        return r.text

    def cancelorder(self,orderid=[],isall=True):
        '''撤单
        '''
        if orderid or isall:
            postdata = {"orderid":orderid,"all":isall}
            try:
                if self.mock:
                    pass
                else:
                    r=requests.post(self.server+'/cancelorder',json=postdata,auth=HTTPBasicAuth(self.token, 'x'))
                    return r.text 
            except:pass
        return 

#     def autobuy(self):
#         '''自动比较利率购买逆回购
#         '''
#         gzrate = self.get_latest_price(self.gznhg).ix[0,"bid1"]*0.1
#         qyzrate = self.get_latest_price(self.qyznhg).ix[0,"bid1"]*0.1
#         account,_ = self.get_position()
#         restmoney = account.ix["可用"]["人民币"]
#         if gzrate > qyzrate:
#             self.buygznhg(restmoney,gzrate)
#             self.buyqyznhg(restmoney%100000,qyzrate)
#         else: 
#             self.buyqyznhg(restmoney,qyzrate)
    
#     def buygznhg(self,money,price):
#         '''自动买入一天期国债逆回购'''
#         num = int(money/100000)*1000 
#         postdata = {"action":3,"priceType":0,"price":price,"amount":num,"symbol":self.gznhg}
#         self.order(postdata)
#         
#     def buyqyznhg(self,money,price):
#         '''自动买入一天期企业债逆回购'''
#         num = int(money/1000)*10
#         postdata = {"action":3,"priceType":0,"price":price,"amount":num,"symbol":self.qyznhg}
#         self.order(postdata)
        
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
    
    @property    
    def info(self):
        info = "ip={}:{},user_name={},pwd={},authdb={}".format(self.__server_ip,self.__server_port,\
            self.__user_name,self.__pwd,self.__authdb)
        return info
            
    def connect(self):
        '''建立数据库的连接
        '''
        _db_session = MongoClient(self.__server_ip, self.__server_port)
        if  self.__user_name:        
            eval("_db_session.{}".format(self.__authdb)).authenticate(self.__user_name,self.__pwd)      
        
        self.client = _db_session
        return _db_session

    def disconnect(self):
        '''断开数据库连接        
        '''
        self._db_session.close()
        return True
    
    def _dbclient(self,db):
        '''返回某个特定数据库的对象
        '''
        return eval("self.client.{}".format(db))

    def update(self,collection,strategy,data={},db="POSITION"):
        '''更新每个策略对应的交易标的持仓数量
           collection: 账号名字
           strategy：策略
           data:{"《交易标的》":"《数量》"}
        '''
        tm = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if len(data)>0:
            bulk = self._dbclient(db)[collection].initialize_ordered_bulk_op()
            for ins,number in data.items():
                d = {"number":number,"status":True,"datetime":tm}
                bulk.find({"name":strategy,"ins":ins}).upsert().update({"$set":d})
            bulk.execute()
    
    def set_false(self,collection,strategy,holdlist=[],drop=False,db="POSITION"):
        '''把不在交易列表内的交易标的设置为非持有状态
                   如果drop为True，则删除该记录
        '''
        self._dbclient(db)[collection].update_many({'name':strategy,'ins':{'$nin':holdlist}}, {"$set":{'status':False,'number':0}})
    
    def get(self,collection,holdlist,db="POSITION"):
        '''获取每个策略对应的交易标的持仓数量
        '''
        filt = {'ins':{'$in':holdlist},"status":True}
        rst = [i for i in self._dbclient(db)[collection].find(filt)]
        return rst