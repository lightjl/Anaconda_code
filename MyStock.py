import tushare as ts
import pandas as pd
from datetime import datetime as dt
import datetime
from pathlib import Path
import time
import numpy as np
import Stock_Base

class MyStock(Stock_Base.Stock_Base):
    def __init__(self, yearEnd = dt.now().year):
        Stock_Base.Stock_Base.__init__(self)
        self.__yearEnd = yearEnd
        self.__initDfs()
        self.__initDfsGrowth()
        self.__initDfsReport()

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

    def research(self):
        dfs_growth      = self.dfs_growth
        self.stockGDF   = dfs_growth[((np.isnan(dfs_growth.nprg4) & (dfs_growth.nprg0>25)) | (dfs_growth.nprg4>25))  &(dfs_growth.nprg1>25)\
                            &(dfs_growth.nprg2>25)&(dfs_growth.nprg3>25)]    # 近4年nprg净利润增长率>25
        self.stockGDF   = self.stockGDF.merge(self.dfs_report, on='code', how='left')
        self.stockGDF['nprg'] \
                        = self.stockGDF.apply(lambda x: x['nprg3'] if pd.isnull(x['nprg4']) else x['nprg4'], axis=1)
        self.stockGDF['pe_report_date'] \
                        = self.stockGDF.apply(lambda x: self.pe(x['code'], x['rec_report_date']), axis=1)
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
print('test')
if __name__ == '__main__':
    test = MyStock()
    test.research()
    test.stockGDF[['code', 'peg_report_date', 'pe_report_date', 'eps', 'nprg', 'price_now', 'rec_report_date',
                   'price_report_date', '涨幅']
                  + ['nprg' + str(i) for i in range(5)]][test.stockGDF.peg_report_date < 0.4].describe()
