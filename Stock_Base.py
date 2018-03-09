import tushare as ts
import pandas as pd
from datetime import datetime as dt
import datetime
from pathlib import Path
import time
import numpy as np
import requests
import json, io

class Stock_Base():
    def __init__(self):
        pass

    def update_data_or_not(self, file, year = dt.now().year-1):
        import os
        yearEnd = dt.now().year-1  #去年
        updateTime = 0
        if ((year==yearEnd and dt.now().month >= 5) or year < yearEnd):
            updateTime = 30
        else:
            updateTime = 1
        updateTime = updateTime*24*60*60
        finfo = file.stat()
        return(( time.time() - finfo.st_mtime) > updateTime)

    def jsonForcast(self, code):
        try:
            to_unicode = unicode
        except NameError:
            to_unicode = str
        urlBase = 'http://emweb.securities.eastmoney.com/PC_HSF10/ProfitForecast/ProfitForecastAjax?code='
        shOrSz = lambda x: ('sz' + code) if code[0] in {'0', '2', '3'} else 'sh' + code
        code = shOrSz(code)
        saveJsonStr = './data/forcast/%s.json' % code
        saveJson = Path(saveJsonStr)
        if (saveJson.exists() and not self.update_data_or_not(saveJson)):  # todo 更新
            with open(saveJsonStr, "r", encoding="utf-8") as data_file:
                stockJson = json.load(data_file)
        else:
            html = requests.get(urlBase + code)
            stockJson = json.loads(html.text)
            saveJson.touch()
            with io.open(saveJsonStr, 'w', encoding='utf8') as outfile:
                str_ = json.dumps(stockJson,
                                  indent=4, sort_keys=True,
                                  separators=(',', ': '), ensure_ascii=False)
                outfile.write(to_unicode(str_))
        return stockJson

    def __zh2float(self, string):
        if ('亿' == string[-1]):
            return float(string[:-1]) * 100000000
        elif ('万' == string[-1]):
            return float(string[:-1]) * 10000

    def nprgForcast(self, code):
        stockJson = self.jsonForcast(code)
        for i in stockJson['Result']['yctj']['data']:
            if (i['rq'].endswith('年预测')):
                if (i['jlr'] == '--' or
                    int(i['jlr'].split('家')[0]) <= 3):
                    return np.NaN
                jlr = self.__zh2float(i['jlr'].split('(')[0])
                #             print('预测： ' + str(jlr) + ' L: ' + str(jlrLast))
                nprg_forcast = round((jlr - jlrLast) / jlrLast * 100, 2)
                break
            jlrLast = self.__zh2float(i['jlr'].split('(')[0])
        return nprg_forcast

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
        else:
            fun = eval('ts.get_%s_data' % dataType)
            df = fun(year, q)
            df.to_csv(saveFileNameStr, index=False, encoding='utf-8')
        return df

    def updateLessThanDays(self, path, days):
        if (path.exists()):
            maxUpdateTime = 24 * 60 * 60 * days
            finfo = path.stat()
            if ((time.time() - finfo.st_mtime) < maxUpdateTime):  # 更新时间小于1天
                return True
        return False

    def price_df(self, date, code):
        saveFileNameStr = './data/stock_%s_%s.csv' % (code, date)
        saveFileName = Path(saveFileNameStr)
        if (self.updateLessThanDays(saveFileName, days=1)):
            df = pd.read_csv(saveFileName)
            return df
        df = ts.get_k_data(code, ktype='D', start=date)
        df.to_csv(saveFileNameStr, index=False, encoding='utf-8')
        return df

    def price_after_report(self, date, code, index):
        return self.price_df(date, code).iloc[index]['close']

    def updatedf1y(self, enddate, path):
        if (dt.now().strftime('%Y-%m-%d') > enddate):
            return path.exists()
        else:   # 小于1天则更新
            return self.updateLessThanDays(path, days=1)

    def price_df_1y(self, startdate, code):
        saveFileNameStr = './data/price/%s_%s_1y.csv' % (code, startdate)
        saveFileName = Path(saveFileNameStr)
        nextyear = int(startdate[0:4]) + 1
        enddate = str(nextyear) + startdate[4:]
        if (self.updatedf1y(enddate, saveFileName) ):  # nextyear >= dt.now().year　and self.updateLessThanDays(saveFileName, days=1)
            df = pd.read_csv(saveFileName)
            if df.empty:
                df = pd.read_csv('./data/price/none.csv')
            return df
        df = ts.get_k_data(code, ktype='D', start=startdate, end=enddate)
        df.to_csv(saveFileNameStr, index=False, encoding='utf-8')
        return df

    def price_after_report_1y(self, date, code, index):
        if self.price_df_1y(date, code).empty:
            return np.NAN
        return self.price_df_1y(date, code).iloc[index]['close']

    def nextDay(self, date):
        tmpDate = dt.strptime(date, '%Y-%m-%d') + datetime.timedelta(days=1)
        return tmpDate.strftime('%Y-%m-%d')


    def get_yjbb_online(self, year):
        urlBase = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?type=NBJB_YJBB_N&token=70f12f2f4f091e459a279469fe49eca5&st=NOTICEDATE&sr=-1&p=1&ps=100000&js=var%20ybkWjYRZ={pages:(tp),data:%20(x)}&filter=(REPORTDATE=^'
        url = urlBase + str(year) + '-12-31^)'
        html = requests.get(url)
        reportlist = (eval('[{}]'.format(html.text.split('[')[1][:-2])))
        reportdf = pd.DataFrame(data=reportlist, columns=reportlist[0].keys())
        reportdf = (reportdf[reportdf.SECUCODE.str[0].isin({'0', '3', '6', '7'})])
        return reportdf

    def get_yjbb_df(self, year):
        fileName = './data/yjbb/{year}.csv'.format(year=year)
        file = Path(fileName)
        if (file.exists()):
            if(not self.update_data_or_not(file, year)):
                reportdf = pd.read_csv(fileName, encoding='utf-8')
                reportdf['SECUCODE'] = reportdf.apply(lambda x: '%06d' % x['SECUCODE'], axis=1)
                return reportdf
        reportdf = self.get_yjbb_online(year)
        reportdf.to_csv(fileName, index=False, encoding='utf-8')
        return reportdf

    def get_index(self, code, enddate, startdate='2010-01-01'):
        # code = hs300
        saveFileNameStr = './data/price/%s_%s.csv' % (code, startdate)
        saveFileName = Path(saveFileNameStr)
        if (saveFileName.exists()):
            df = pd.read_csv(saveFileName)
            if ( (not df.empty) or df['date'].max() < enddate):  # 需要更新
                dfnew = ts.get_k_data(code, start=self.nextDay(df['date'].max()))
                if (not dfnew.empty):
                    df = pd.concat([df, dfnew])
                    df.to_csv(saveFileNameStr, index=False, encoding='utf-8')
        else:
            df = ts.get_k_data(code, start=startdate)
            df.to_csv(saveFileNameStr, index=False, encoding='utf-8')
        return df

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
            df = self.get_stock_basics(self.nextDay(date))
            df.to_csv(saveFileNameStr, encoding='utf-8')
            return df

    def basics_df(self, date=None):
        if date is None:
            saveFileNameStr = './data/basics/%s.csv' % (date)
            saveFileName = Path(saveFileNameStr)
            if (self.updateLessThanDays(saveFileName, days=1)):
                df = pd.read_csv(saveFileName)
                df['code'] = df['code'].apply(lambda x: '%06d' % int(x))
                df.set_index('code', inplace=True, drop=True)
                return df
            df = ts.get_stock_basics()
            df.to_csv(saveFileNameStr, encoding='utf-8')
            return df
        df = self.get_stock_basics(date)
        return df

    def StockCode2XSH(self, code):
        if code[0] in {'0', '2', '3'}:
            return code + '.XSHE'
        else:
            return code + '.XSHG'
