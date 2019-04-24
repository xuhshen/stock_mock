'''
Created on 2018年9月3日

@author: 04yyl
'''
'''商品组合交易模拟 15min 交易周期
'''
from pytdx.hq import TdxHq_API
from cfg import logger,STOCK_IP_SETS
from gevent import monkey;monkey.patch_all()
from trade import trade
import os

class SP(object):
    def __init__(self,userid="account4",server="http://192.168.118.1:5000"):
        self.server = server
        self.userid = userid
        self.api = TdxHq_API(heartbeat=True)
        self.trader = None
        self.trading = False
        
        self.TDX_IP_SETS = STOCK_IP_SETS
    
    def connect(self):
        for ip in self.TDX_IP_SETS:
            try:
                if self.api.connect(ip, 7709):
                    return 
            except:
                pass
    
    def disconnect(self):
        logger.info("[DISCONNECT]:start disconnect !!!!!")
        self.api.disconnect()
        logger.info("[DISCONNECT]:disconnect finished !!!!!")
    
    def initial(self):
        '''每天初始化设置
        '''
        logger.info("[INITIAL]:start initial !!!!!")
        logger.info("[INITIAL]:try to create connect... ")
        self.connect()
        self.trader = trade(UserID=self.userid,api=self.api,mock=False,server=self.server)
        logger.info("[INITIAL]:connect successful!")
        logger.info("[INITIAL]:initial finished !!!!!")
        
    def run(self):
        logger.info("[RUN]:start run !!!!!")
        self.trader.autobuy()
        logger.info("[RUN]:run finished !!!!!")
        
if __name__ == '__main__':
    from apscheduler.schedulers.blocking import BlockingScheduler
    account = os.environ.get('ACCOUNT',"account2")
    server=os.environ.get('SERVER',"http://192.168.0.100:5000")

    s = SP(userid=account,server=server)
    s.initial()
    sched = BlockingScheduler()
    sched.add_job(s.initial,'cron', day_of_week='0-4', hour='14',minute='40',misfire_grace_time=60)
      
    sched.add_job(s.run,'cron', day_of_week='0-4', hour='14',minute='50,55',misfire_grace_time=60)
      
    sched.add_job(s.disconnect,'cron', day_of_week='0-4', hour='15',minute='15',misfire_grace_time=60)
       
    sched.start()
    
    
    
    
