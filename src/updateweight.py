'''
Created on 2019年4月26日

@author: 04yyl

每个月初定时更行所有股票的流通市值以及对应的行业股票
'''
import tushare as ts
from trade.td import MongoDB
from pytdx.hq import TdxHq_API
import math
from cfg import logger,FILE_INCON,FILE_TDXHY,FILE_TDXZS,STOCK_IP_SETS,\
                ONLINE_FILE_INCON,ONLINE_FILE_TDXHY,ONLINE_FILE_TDXZS
import re,os
import sys
import shutil 
import urllib.request as request

class basic(object):
    def __init__(self,ip="192.168.0.106",online=True):
        self.mongo = MongoDB(ip=ip)
        self.api = TdxHq_API(heartbeat=True)
        self.TDX_IP_SETS = STOCK_IP_SETS
        self.collection = "BASIC_INFO"
        self.file_incon = FILE_INCON
        self.file_tdxhy = FILE_TDXHY
        self.file_tdxzs = FILE_TDXZS
        
        self.online = online #版本库获取最新文件

    def updatelocalfile(self):
        with open('tmp1', 'wb') as f:
            r = request.urlopen(ONLINE_FILE_INCON).read()
            f.write(r)
            if b"#ZJHHY" in r: #内容检查
                shutil.copy("tmp1",self.file_incon)
    
        with open('tmp2', 'wb') as f:
            r = request.urlopen(ONLINE_FILE_TDXHY).read()
            f.write(r)
            if b"000001" in r: #内容检查
                shutil.copy("tmp2",self.file_tdxhy)
            
        with open('tmp3', 'wb') as f:
            r = request.urlopen(ONLINE_FILE_TDXZS).read()
            f.write(r)
            if b"880472" in r: #内容检查
                shutil.copy("tmp3",self.file_tdxzs)
        return 

    def connect(self):
        self.mongo.connect()
        for ip in self.TDX_IP_SETS:
            try:
                if self.api.connect(ip, 7709):
                    return 
            except:
                pass
    
    def disconnect(self):
        self.mongo.disconnect()
        self.api.disconnect()
    
    def get_stock_basics(self):
        ''' code,代码                                    name,名称                       industry,所属行业             
            outstanding,流通股本(亿)    totals,总股本(亿)    totalAssets,总资产(万)
            liquidAssets,流动资产             area,地区                       pe,市盈率
            fixedAssets,固定资产               reserved,公积金           reservedPerShare,每股公积金
            esp,每股收益                               bvps,每股净资                pb,市净率
            timeToMarket,上市日期             undp,未分利润                perundp, 每股未分配
            rev,收入同比(%)            profit,利润同比(%)   gpr,毛利率(%)
            npr,净利润率(%)            holders,股东人数
        '''
        
        df = ts.get_stock_basics()
        df = df.apply(lambda x:round(x,6) if isinstance(x,float) else x) #处理数据精度
        df.loc[:,"ST"] = df["name"].map(lambda x:True if "ST" in x.upper() else False)
        df.loc[:,"code"] = df.index
        
        return df
    
    def save_basic(self,df):
        '''保存基本信息到数据库
        '''
        self.mongo._dbclient(self.mongo.db)[self.collection].ensure_index("code", unique=True)
        bulk = self.mongo._dbclient(self.mongo.db)[self.collection].initialize_ordered_bulk_op()
        data = df.to_dict("record")
        for item in data:
            filt = {"code":item["code"]}
            bulk.find(filt).upsert().update({"$set":item})
        bulk.execute()
    
    def calculateltsz(self,df):
        '''计算流通市值
           output:{code:[gb,value}
        '''

        df.loc[:,"market"] = df.index.map(lambda x:0 if x[0] in ["0","3"] else 1)
        
        rst = df[df["outstanding"]>0][["market","code",]].values
        pn = 80
        total = rst.shape[0]
        #更新流通股本为0的股票的价格为0
        prices = {code:0 for code in df[df["outstanding"]==0]["code"].values}
        
        liutonggb = {code:ltgb for code,ltgb in df[["code","outstanding"]].values}
        zeros = []
        
        #更新流通股本大于0的股票
        for idx in range(math.ceil(total/pn)):
            start = idx*pn
            end = start+pn
            logger.info("[RUN]: get latest price for {}:{}".format(start,end))
            pankou = self.api.get_security_quotes(rst[start:end])
            for item in pankou:
                prices[item["code"]] = round(item["price"],2) #注意这里需要处理下数据有效位，否则保存数据库的时候会报错
                if item["price"] ==0 :zeros.append(item["code"])
        
        #重新更新流通股份大于0,但是盘口价格为0的股票
        for stock in zeros:
            mk = 0 if stock[0] in ["0","3"] else 1
            logger.info("[RUN]: get latest price for {}".format(stock))
            prices[stock] = round(self.api.get_security_bars(6,mk,stock,0,1)[0]["close"],2)
        
        #计算流通市值
        liutongvalues = {code:[gb,gb*prices[code]] for code,gb in liutonggb.items()}
        
        return liutongvalues
        
    def updatevalue2db(self,data):
        
        bulk = self.mongo._dbclient(self.mongo.db)[self.collection].initialize_ordered_bulk_op()
        for k,v in data.items():
            filt = {"code":k}
            bulk.find(filt).upsert().update({"$set":{"liutongguben":v[0],"liutongvalue":v[1]}})
        bulk.execute()
        
    def updateweight2db(self,data):
        bulk = self.mongo._dbclient(self.mongo.db)[self.collection].initialize_ordered_bulk_op()
        for k,v in data.items():
            filt = {"code":k}
            bulk.find(filt).upsert().update({"$set":{"hyweight":v[0],"hyname":v[1],"hycode":v[2]}})
        bulk.execute()
    
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
 
    def _get_tdxhy(self,):
        '''获取股票和行业对应列表
        '''
        stocklist = [i["code"] for i in self.mongo._dbclient(self.mongo.db)[self.collection].find(projection={"code":1,"_id":0})]
         
        rst = self._get_incon()
        f= open(self.file_tdxhy, "rb")
        data = f.read().decode("gbk", 'ignore').rstrip("\x00").replace("\r\n","\n").strip("\n").split("\n")
                 
        for i in data:
            _,code,tdxhy,_,_ = i.split("|")
            if tdxhy != "T00" and code in stocklist:
                rst[tdxhy].append(code)
        return rst
 
    def get_tdxzs(self,):
        '''生成通达性版块代码对应股票列表
        '''
        dct = {}
        rst = self._get_tdxhy()
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
    
    def calculateweight(self,liutongvalues):
        '''计算每个股票在对应行业的权重
           output:{code:[weight,hyname,hycode]}
        '''
        rst = self.get_tdxzs()
        weights = {}
        for hy in rst.values():
            totalvalue = sum([liutongvalues[stock][1] for stock in hy["stocklist"]])
            for stock in hy["stocklist"]:
                weights[stock] = [round(liutongvalues[stock][1]/totalvalue,5),hy["name"],str(hy["code"])]
        return weights
    
    def run(self):
        '''更新流通股本和行业板块权重到数据库
        '''
        funcname = sys._getframe().f_code.co_name.upper() #获取当前函数名
        
        if self.online:
            logger.info("[{}]: update local config file! ".format(funcname))
            self.updatelocalfile()
            logger.info("[{}]: update local config file finished! ".format(funcname))
        
        logger.info("[{}]: create connect to tdx and mongo! ".format(funcname))
        self.connect()
        logger.info("[{}]: create connect to tdx and mongo finished! ".format(funcname))
        
        df = self.get_stock_basics()
        self.save_basic(df)
        
        logger.info("[{}]: get latest price and calculate liutong value start ! ".format(funcname))
        liutongvalues = self.calculateltsz(df)
        logger.info("[{}]: get latest price and calculate liutong value finished ! ".format(funcname))
         
        logger.info("[{}]: update liu tong gu ben to db start ! ".format(funcname))
        self.updatevalue2db(liutongvalues)
        logger.info("[{}]: update liu tong gu ben to db finished ! ".format(funcname))
        
        logger.info("[{}]: calculate hang ye weight! ".format(funcname)) 
        weights = self.calculateweight(liutongvalues) 
        logger.info("[{}]: update hang ye weight to db start ! ".format(funcname)) 
        self.updateweight2db(weights)
        logger.info("[{}]: update hang ye weight to db finished ! ".format(funcname)) 
        
        logger.info("[{}]: disconnect from tdx and mongo start ! ".format(funcname))
        self.disconnect()
        logger.info("[{}]: disconnect from tdx and mongo finished ! ".format(funcname))
    

if __name__ == "__main__":
    from apscheduler.schedulers.blocking import BlockingScheduler
    ip = os.environ.get('IP',"localhost")
    
    bs = basic(ip=ip)
    bs.run()
     
    sched = BlockingScheduler()
#     sched.add_job(bs.run,'cron', month='*',day="1",hour='9',minute='30',misfire_grace_time=60)
    sched.add_job(bs.run,'cron', day_of_week='0-4',hour='9',minute='30',misfire_grace_time=60) #每个工作日更新,程序在每次新开仓买入的时候去更新权重
      
    sched.start()
    
    
    
    