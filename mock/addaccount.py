'''
Created on 2019年3月7日

@author: 04yyl
'''
from simulation import MongoDB
import os 
from cfg import logger

class Tool(MongoDB):
    
    def addaccount(self,name,total):
        rst = {"hold":0,"rest":total,"total":total}
        self._dbclient(self.db)[self.account_collection].update_one({"account":name},{"$set":rst},upsert=True)
        
    def getacchistory(self,name):
        pass 
    
    def get(self,name):  
        '''
        '''  
        rst = self._dbclient(self.db)[name].find()
        for i in rst:
            logger.info(i)
        
if __name__ == '__main__':
    
    ip = os.environ.get('IP',"stock_mongo")
    name = os.environ.get('name',None)
    money = os.environ.get('money',10000000)
    action = os.environ.get('action',"addaccount")
    
    if not name:
        logger.info("please add account name!!!! ")
    else:
        tool = Tool(ip=ip)
        tool.connect()
        if action == "addaccount":
            tool.addaccount(name,money)
        elif action == "getacchistory":
            tool.getacchistory(name)
        elif action == "get":
            tool.get(name)
        tool.disconnect()    
            