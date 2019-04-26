'''
Created on 2019年4月26日

@author: 04yyl

每个月初定时更行所有股票的流通市值以及对应的行业股票
'''
import tushare as ts
from trade.td import MongoDB
from pytdx.hq import TdxHq_API
from cfg import logger,FILE_INCON,FILE_TDXHY,FILE_TDXZS,STOCK_IP_SETS


class basic(object):
    def __init__(self):
        self.mongodb = MongoDB()
        self.api = TdxHq_API(heartbeat=True)
        self.TDX_IP_SETS = STOCK_IP_SETS
        
    
    def connect(self):
        self.mongodb.connect()
        for ip in self.TDX_IP_SETS:
            try:
                if self.api.connect(ip, 7709):
                    return 
            except:
                pass
    
    def disconnect(self):
        self.mongodb.disconnect()
        self.api.disconnect()
    
    
    def get_stock_basics(self):
        ''' code,代码
            name,名称
            industry,所属行业
            area,地区
            pe,市盈率
            outstanding,流通股本(亿)
            totals,总股本(亿)
            totalAssets,总资产(万)
            liquidAssets,流动资产
            fixedAssets,固定资产
            reserved,公积金
            reservedPerShare,每股公积金
            esp,每股收益
            bvps,每股净资
            pb,市净率
            timeToMarket,上市日期
            undp,未分利润
            perundp, 每股未分配
            rev,收入同比(%)
            profit,利润同比(%)
            gpr,毛利率(%)
            npr,净利润率(%)
            holders,股东人数
        '''
        df = ts.get_stock_basics()
        return df
    
    def calculateltsz(self):
        '''计算流通市值
        '''
        df = self.get_stock_basics()
        ndf = df[df["outstanding"]>0]
        ndf.loc[:,"market"] = ndf.index.map(lambda x:0 if x[0] in ["0","3"] else 1)
        ndf.loc[:,"stock"] = ndf.index
        
    
    def update2db(self):
        pass
    
    
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