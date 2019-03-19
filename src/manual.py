from trade import trade
from pytdx.hq import TdxHq_API
from cfg import logger,STOCK_IP_SETS
import os

api = TdxHq_API()

def connect(api):
    for ip in STOCK_IP_SETS:
        try:
            if api.connect(ip, 7709):
                return 
        except:
            pass
connect(api)

if __name__ == '__main__':
    userid = os.environ.get('userid',"test1")
    buylist = eval(os.environ.get('buylist',"[]"))
    selllist = eval(os.environ.get('selllist',"[]"))
    
    trader = trade(UserID=userid,api=api,mock=True)
    
    _,holdlists = trader.position()
    if holdlists.shape[0]>0:
        holdlists.set_index("证券代码",inplace=True)
    
    for stock,number in buylist:
        logger.info("buy: {} number:{}".format(stock,number))
        trader.buy(stock, number)
    
    for stock,number in selllist:
        try:
            h_number = holdlists.ix[stock]["参考持股"]
        except:
            h_number = 0
        
        if number >=h_number:
            logger.info("holdnumber is:{}".format(h_number)) 
            logger.info("sell: {} number:{}".format(stock,h_number))
            trader.sell(stock, h_number)
        else:
            logger.info("sell: {} number:{}".format(stock,number))
            trader.sell(stock, number)
    
    