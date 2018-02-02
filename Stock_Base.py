import tushare as ts
import pandas as pd
from datetime import datetime as dt
import datetime
from pathlib import Path
import time
import numpy as np


class Stock_Base():
    def __init__(self):
        pass

    def update_data_or_not(self, fileName, year):
        import os
        yearEnd = dt.now().year-1  #去年
        updateTime = 0
        if ((year==yearEnd and dt.now().month >= 5) or year < yearEnd):
            updateTime = 30
        else:
            updateTime = 1
        updateTime = updateTime*24*60*60
        finfo = fileName.stat()
        return(( time.time() - finfo.st_mtime) > updateTime)

    def get_data(self, dataType, year, q=4): #年报
        typeAll = {'profit', 'report', 'growth'}
        if (dataType not in typeAll):
            print('%s not in %s' %(type, typeAll,))
            return
        saveFileNameStr = './data/%s%02d%s.csv' % (year, q, dataType)
        saveFileName = Path(saveFileNameStr)
        if saveFileName.exists():
            df = pd.read_csv(saveFileName)#, encoding='gb18030')
            # return df
        if (self.update_data_or_not(saveFileName, year)):  # 如果需要更新
            fun = eval('ts.get_%s_data' % dataType)
            dfnew = fun(year, q)
            df = pd.concat([df[~df['code'].isin(dfnew['code'].values)],dfnew])
            df.to_csv(saveFileNameStr, index=False, encoding='utf-8')
        return df

    def price_df(self, date, code):
        saveFileNameStr = './data/stock_%s_%s.csv' % (code, date)
        saveFileName = Path(saveFileNameStr)
        if (saveFileName.exists()):
            maxUpdateTime = 24 * 60 * 60
            finfo = saveFileName.stat()
            if ((time.time() - finfo.st_mtime) < maxUpdateTime):  # 更新时间小于1天
                df = pd.read_csv(saveFileName)
                return df
        df = ts.get_k_data(code, ktype='D', start=date)
        df.to_csv(saveFileNameStr, index=False, encoding='utf-8')
        return df

    def price_after_report(self, date, code, index):
        return self.price_df(date, code).iloc[index]['close']

    def get_stock_basics(self, date):
        saveFileNameStr = './data/basics/%s.csv' % (date)
        saveFileName = Path(saveFileNameStr)
        if (saveFileName.exists()):
            df = pd.read_csv(saveFileName)
            df['code'] = df['code'].apply(lambda x: '%06d' % int(x))
            df.set_index('code', inplace=True, drop=True)
            return df
        try:
            df = ts.get_stock_basics(date)
            df.to_csv(saveFileNameStr, encoding='utf-8')
            return df
        except:
            tmpDate = dt.strptime(date, '%Y-%m-%d') + datetime.timedelta(days=1)
            date = tmpDate.strftime('%Y-%m-%d')
            df = self.get_stock_basics(date)
            df.to_csv(saveFileNameStr, encoding='utf-8')
            return df

    def basics_df(self, date):
        if date is None:
            df = ts.get_stock_basics()
            return df
        df = self.get_stock_basics(date)
        return df
