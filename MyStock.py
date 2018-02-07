import tushare as ts
import pandas as pd
from datetime import datetime as dt
import datetime
from pathlib import Path
import time
import numpy as np
import Stock_Base
from importlib import reload

reload(Stock_Base)

class MyStock(Stock_Base.Stock_Base):
    def __init__(self, yearEnd = dt.now().year):
        Stock_Base.Stock_Base.__init__(self)
        self.__yearEnd = yearEnd
        self.__initYJBB()
#         self.__initDfs()
#         self.__initDfsGrowth()
#         self.__initDfsReport()

    def YearEnd(self):
        return self.__yearEnd

    def ChangeYearEnd(self, yearEnd):
        if (int(yearEnd) != self.__yearEnd):
            self.__yearEnd = int(yearEnd)
            self.__initYJBB()
#             self.__initDfs()
#             self.__initDfsGrowth()
#             self.__initDfsReport()

    def __initYJBB(self):
        self.yjbb = {}
        for y in range(self.__yearEnd - 5, self.__yearEnd):
            dftmp = self.get_yjbb_df(y)
            dftmp['SJLTZ'] = dftmp.apply(lambda x: np.NaN if x['SJLTZ'] == '-' else float(x['SJLTZ']), axis=1)
            dftmp['NOTICEDATE'] = dftmp.apply(lambda x: x['NOTICEDATE'].split('T')[0] , axis=1)
            self.yjbb[y] = dftmp.rename(
                    columns={i: i + str(y - self.__yearEnd + 5) for i in dftmp.columns if i not in {'SECUCODE'}})
        self.yjbbs = pd.DataFrame(columns=['SECUCODE'])
        for i in range(self.__yearEnd-5, self.__yearEnd):
            self.yjbbs = self.yjbbs.merge(self.yjbb[i], on=['SECUCODE'], how='outer')

        self.yjbbs['rec_report_date'] = self.yjbbs.apply(
            lambda x:  x['NOTICEDATE3'] if pd.isnull(x['NOTICEDATE4']) else x['NOTICEDATE4'], axis=1)

    def __initDfs(self):
        self.dfs = {'profit':{}, 'report':{}, 'growth':{}}
        types = {'profit', 'report', 'growth'}
        for t in types:
            for y in range(self.__yearEnd - 5, self.__yearEnd):
                self.dfs[t][y] = self.get_data(t, y).rename(
                    columns={i: i + str(y - self.__yearEnd + 5) for i in self.get_data(t, y).columns if i not in {'code'}})
                # dfs[t][y]['code'] = dfs[t][y]['code'].astype(int)
                self.dfs[t][y]['code'] = self.dfs[t][y]['code'].apply(lambda x: '%06d' % int(x))

    def __initDfsGrowth(self):
        self.dfs_growth = pd.DataFrame(columns=['code'])
        for i in range(self.__yearEnd-5, self.__yearEnd):
            self.dfs_growth = self.dfs_growth.merge(self.dfs['growth'][i], on=['code'], how='outer')

    def __initDfsReport(self):
        self.dfs_report = pd.DataFrame(columns=['code'])
        for i in range(self.__yearEnd - 2, self.__yearEnd):
            self.dfs_report = self.dfs_report.merge(self.dfs['report'][i], on=['code'], how='outer')
        # dfs['profit'][2013].merge(dfs['profit'][2014], on=['code', 'name'], how='outer')
        self.dfs_report = self.dfs_report.drop_duplicates()
        self.dfs_report['eps'] = self.dfs_report.apply(lambda x: x['eps3'] if pd.isnull(x['eps4']) else x['eps4'], axis=1)
        self.dfs_report['rec_report_date'] = self.dfs_report.apply(
            lambda x: '%d-%s' % (self.__yearEnd - 1, x['report_date3']) if pd.isnull(x['report_date4']) else '%d-%s' % (
                self.__yearEnd, x['report_date4']), axis=1)

    def pe(self, code, date=None):
        if code not in self.basics_df(date).index:
            return np.NaN
        return self.basics_df(date).loc[code, 'pe']

    def watch(self):
        #     SJLTZ  净利润	同比增长(%)
        yjbbs = self.yjbbs
        self.stockGDF = self.yjbbs[((np.isnan(yjbbs.SJLTZ4) & (yjbbs.SJLTZ0>25)) | (~np.isnan(yjbbs.SJLTZ4) & (yjbbs.SJLTZ4>25))) \
                                     &(yjbbs.SJLTZ1>25) &(yjbbs.SJLTZ2>25)&(yjbbs.SJLTZ3>25)] # 近4年SJLTZ净利润同比增长率>25
        self.stockGDF.loc[:,('SJLTZ')] \
                        = self.stockGDF.apply(lambda x: x['SJLTZ3'] if pd.isnull(x['SJLTZ4']) else x['nprg4'], axis=1)

        self.stockGDF.loc[:,('pe_report_date')] \
                        = self.stockGDF.apply(lambda x: self.pe(x['SECUCODE'], self.nextDay(x['rec_report_date'])), axis=1)

        self.stockGDF.loc[:,('eps_ondate')] = self.stockGDF.apply(lambda x: x['EPSJB3'] if (pd.isnull(x['EPSJB4'])) else x['EPSJB4'], axis=1)

        self.stockGDF.loc[:,('peg_report_date')] \
                        = round(self.stockGDF['pe_report_date']/self.stockGDF['SJLTZ'],2)

        self.stockGDF.loc[:,('price_report_date')] \
                        = self.stockGDF.apply(lambda x: \
                            self.price_after_report(x['rec_report_date'], x['SECUCODE'], index=0), axis=1)
        self.stockGDF.loc[:,('price_now')] \
                        = self.stockGDF.apply(lambda x: \
                            self.price_after_report(x['rec_report_date'], x['SECUCODE'], index=-1), axis=1)
        self.stockGDF.loc[:,('涨幅')] \
                        = (self.stockGDF['price_now'] - self.stockGDF['price_report_date']) / self.stockGDF['price_report_date']
        self.stockGDF = self.stockGDF.merge(self.basics_df()[['pe']], left_on='SECUCODE', right_index=True)
        self.stockGDF.loc[:,('peg')] \
                        = round(self.stockGDF['pe']/self.stockGDF['SJLTZ'],2)

    def research(self):  #废弃
        dfs_growth      = self.dfs_growth
        self.stockGDF   = dfs_growth[((np.isnan(dfs_growth.nprg4) & (dfs_growth.nprg0>25)) | (~np.isnan(dfs_growth.nprg4) & (dfs_growth.nprg4>25))) \
                                     &(dfs_growth.nprg1>25) &(dfs_growth.nprg2>25)&(dfs_growth.nprg3>25)]    # 近4年nprg净利润增长率>25
        self.stockGDF   = self.stockGDF.merge(self.dfs_report, on='code', how='left')
        self.stockGDF['nprg'] \
                        = self.stockGDF.apply(lambda x: x['nprg3'] if pd.isnull(x['nprg4']) else x['nprg4'], axis=1)

        self.stockGDF['pe_report_date'] \
                        = self.stockGDF.apply(lambda x: self.pe(x['code'], self.nextDay(x['rec_report_date'])), axis=1)
        self.stockGDF['peg_report_date'] \
                        = round(self.stockGDF['pe_report_date']/self.stockGDF['nprg'],2)
        self.stockGDF['price_report_date'] \
                        = self.stockGDF.apply(lambda x: \
                            self.price_after_report(x['rec_report_date'], x['code'], index=0), axis=1)
        self.stockGDF['price_now'] \
                        = self.stockGDF.apply(lambda x: \
                            self.price_after_report(x['rec_report_date'], x['code'], index=-1), axis=1)
        self.stockGDF['涨幅'] \
                        = (self.stockGDF['price_now'] - self.stockGDF['price_report_date']) / self.stockGDF['price_report_date']
        self.stockGDF = self.stockGDF.merge(self.basics_df()[['pe']], left_on='code', right_index=True)
        self.stockGDF['peg'] \
                        = round(self.stockGDF['pe']/self.stockGDF['nprg'],2)

    def BackTesting(self, dateDT):
        date = dateDT.strftime('%Y-%m-%d')
        yearThen = dateDT.strftime('%Y')
        self.ChangeYearEnd(yearThen)
        self.stock = self.yjbbs
        self.universe = \
            self.stock[['SECUCODE', 'SECUNAME3', 'rec_report_date'] + ['SJLTZ' + str(i) for i in range(5)] + ['EPSJB3', 'EPSJB4']] \
                 [
                (( (self.stock.rec_report_date > date) | (self.stock.rec_report_date.str[0:4] < yearThen) ) & (self.stock.SJLTZ0 > 25) |  # 上年年报未出 注意是否同一年
                 ((self.stock.rec_report_date <= date) & (self.stock.rec_report_date.str[0:4] == yearThen) & (self.stock.SJLTZ4 > 25)))  # 上年年报已出
                & (self.stock.SJLTZ1 > 25) & (self.stock.SJLTZ2 > 25) & (self.stock.SJLTZ3 > 25)
                ]
        # print(len(self.universe))
        Report4OrNot = lambda reportDate, date: (reportDate <= date) and (reportDate[0:4] == date[0:4])
        self.universe['nprg_ondate'] = self.universe.apply(lambda x: float(x['SJLTZ4']) if (Report4OrNot(x['rec_report_date'], date)) else float(x['SJLTZ3']), axis=1)

        self.universe['eps_ondate'] = self.universe.apply(lambda x: float(x['EPSJB4']) if (Report4OrNot(x['rec_report_date'], date)) else float(x['EPSJB3']), axis=1)
        # 精准预测
        self.universe = \
            self.stock[['SECUCODE', 'SECUNAME3', 'rec_report_date'] + ['SJLTZ' + str(i) for i in range(5)] + ['EPSJB3', 'EPSJB4']] \
                [                    (self.stock.SJLTZ1 > 25) & (self.stock.SJLTZ2 > 25) & (self.stock.SJLTZ3 > 25) & \
                    ((np.isnan(self.stock.SJLTZ4) & (self.stock.SJLTZ0 > 25)) | (~np.isnan(self.stock.SJLTZ4) & (self.stock.SJLTZ4 > 25)))
                ]
        self.universe.loc[:,('nprg_ondate')] = self.universe.apply(lambda x: float(x['SJLTZ3']) if (pd.isnull(x['SJLTZ4'])) else float(x['SJLTZ4']), axis=1)
        self.universe.loc[:,('eps_ondate')] = self.universe.apply(lambda x: float(x['EPSJB3'])  if (pd.isnull(x['EPSJB4'])) else float(x['EPSJB4']), axis=1)


if __name__ == '__main__':
    test = MyStock()
    print(test.basics_df())
    #test.research()
    #test.stockGDF[['code', 'peg_report_date', 'pe_report_date', 'eps', 'nprg', 'price_now', 'rec_report_date',
                  #  'price_report_date', '涨幅']
                  # + ['nprg' + str(i) for i in range(5)]][test.stockGDF.peg_report_date < 0.4].describe()

