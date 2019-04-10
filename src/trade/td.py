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
from cfg import logger

class trade(object):
    def __init__(self,UserID="xuhshen",api=None,server="http://192.168.0.100:5000",mock=True):
        self.api = api
        self.server = server
        self.account = UserID
        self.gznhg = "204001" #一天期国债逆回购代码
        self.qyznhg = "131810" #一天期企业债逆回购代码
        
        self.mock = mock
        if mock:
            self.mongodb = MongoDB()
            self.mongodb.connect()
    
    @property    
    def token(self):
        user=self.account
        passwd=self.account
        r = requests.get(self.server+'/api/token', auth=HTTPBasicAuth(user, passwd))
        token = json.loads(r.text)["token"]
        return token
    
    def position(self):
        if self.mock:
            accountinfo = pd.DataFrame(self.mongodb.getaccount(self.account)) #获取账户总资金
            accountinfo.loc[:,"index"] = "总资产"
            accountinfo.loc[:,"人民币"] = accountinfo["total"]
            accountinfo.set_index("index",inplace=True)
            
            holdlists = pd.DataFrame(self.mongodb.getholdlist(self.account)) #获取持仓股份信息
            try:
                holdlists.loc[:,"证券数量"] = holdlists["number"]
                holdlists.loc[:,"可卖数量"] = holdlists["number"]
                holdlists.loc[:,"参考持股"] = holdlists["number"]
                holdlists.loc[:,"证券代码"] = holdlists["code"]
                holdlists.set_index("code",inplace=True)
            except:pass
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
        rst = self.api.get_security_quotes([(market, code)])[0]
        
        return rst
    
    def set_buy_price(self,stock):
        market = self.get_latest_price(stock)
        buy_price = market["bid1"]
        ask1 = market["ask1"]
        return buy_price,ask1
    
    def set_sell_price(self,stock):
        market = self.get_latest_price(stock)
        sell_price = market["ask1"]
        buy1 = market["bid1"]
        return sell_price,buy1            
    
    def buy(self,stock,number,jump=0):
        '''买入股票
        '''
        price,ask1 = self.set_buy_price(stock)
        if ask1 <=0 : #股票已经涨停，不买入，因为没法买入
            return 
        if price ==0:
            price = ask1 # 股价已经跌停，按卖方价买入
        postdata={"action":0,"priceType":0,"price":price+jump,"amount":number,"symbol":stock}
        self.order(postdata)
        
    def sell(self,stock,number,jump=0):
        price,buy1 = self.set_sell_price(stock)
        if buy1 <=0 : #股票已经跌停，不卖出,因为没法卖出,或者股价已经涨停，不卖
            return 
        if price == 0:
            price = buy1 # 股价已经涨停，按买方价卖出
        postdata={"action":1,"priceType":0,"price":price-jump,"amount":number,"symbol":stock}
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
            logger.info(postdata)
            self.mongodb.updateholdlist(self.account,postdata) #更新持仓信息
            self.mongodb.add_operate_history(self.account,postdata) #添加操作记录
        else:
            if postdata["symbol"] in ["510500"]:postdata["price"] *= 0.1 #etf调整价格
                
            r=requests.post(self.server+'/orders',json=postdata,auth=HTTPBasicAuth(self.token, 'x'))
            return r.text
        return 

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

    def autobuy(self):
        '''自动比较利率购买逆回购
        '''
        gzrate = self.get_latest_price(self.gznhg).ix[0,"bid4"]*0.1
        qyzrate = self.get_latest_price(self.qyznhg).ix[0,"bid4"]*0.1
        account,_ = self.get_position()
        restmoney = account.ix["可用"]["人民币"]
        if gzrate > qyzrate:
            self.buygznhg(restmoney,gzrate)
            self.buyqyznhg(restmoney%100000,qyzrate)
        else: 
            self.buyqyznhg(restmoney,qyzrate)
     
    def buygznhg(self,money,price):
        '''自动买入一天期国债逆回购'''
        num = int(money/100000)*1000 
        postdata = {"action":3,"priceType":0,"price":price,"amount":num,"symbol":self.gznhg}
        self.order(postdata)
         
    def buyqyznhg(self,money,price):
        '''自动买入一天期企业债逆回购'''
        num = int(money/1000)*10
        postdata = {"action":3,"priceType":0,"price":price,"amount":num,"symbol":self.qyznhg}
        self.order(postdata)
        
class MongoDB(object):
    def __init__(self,
                    ip="stock_mongo", #mongo db 数据库docker 容器名
#                     ip="192.168.0.106",
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
        self.db = "stock_mock"   #数据库
        self.account_collection = "account"  #保存各个账户的当前资金信息
        self.his_collection = "operate_history"  #保存各个账户的当前资金信息
    
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

    def getaccount(self,account):
        clt = [i for i in self._dbclient(self.db)[self.account_collection].find({"account":account})]
        return clt
    
    def getholdlist(self,account):
        collection = "holdlist_"+account
        clt = self._dbclient(self.db)[collection].find({"number":{"$gt":0}})
        rst = [i for i in clt]
        return rst
    
    def updateholdlist(self,account,postdata):
        
        stock = postdata["symbol"]
        collection = "holdlist_"+account
        filt = {"code":stock}
        
        tm = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        dt = {"update_datetime":tm}
        try:
            rst = self._dbclient(self.db)[collection].find(filt)[0]
        except:
            rst = {"cost":0,"number":0}
            
        if postdata["action"] == 0:#买入
            changemoney = postdata["amount"]*postdata["price"]
            dt["cost"] = (rst["number"]*rst["cost"]+changemoney) /(rst["number"]+postdata["amount"])
            dt["number"] = rst["number"]+postdata["amount"]
        elif postdata["action"] == 1: #卖出
            changemoney = -postdata["amount"]*postdata["price"]*0.9985 #卖出时按照千分之一点五计算手续费
            if rst["number"] == postdata["amount"]:
                dt["cost"] = 0
            else:
                dt["cost"] = (rst["number"]*rst["cost"]+changemoney)/(rst["number"]-postdata["amount"])
            
            dt["number"] = rst["number"]-postdata["amount"]  
        
        self._dbclient(self.db)[collection].update_one(filt,{"$set":dt},upsert=True) #更新持仓股票
        
        filt = {"account":account}
        self._dbclient(self.db)[self.account_collection].update_one(filt,{"$inc":{"rest":-changemoney}})#更新账户剩余资金
        
        return                    
        
    def add_operate_history(self,account,postdata): #添加操作记录  
        data = postdata
        data["account"] = account
        data["datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._dbclient(self.db)[self.his_collection].insert_one(data)


