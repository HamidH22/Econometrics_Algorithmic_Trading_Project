# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 13:22:54 2020

@author: Hamid
"""

from __future__ import (absolute_import, division, print_function, unicode_literals)
from binance.client import Client
import pandas as pd
import os
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind

key = ""
secret = ""
client = Client(key, secret)
currencies = ['BTCUSDT', 'BCHUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT']

bitcoin = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_15MINUTE, "20 Oct, 2020")

df = {}    
timestamp = []
timeperiod = '1h'
for crypto in currencies:
    timestamp.append(client._get_earliest_valid_timestamp(crypto, timeperiod))

timestamp = max(timestamp)
startdate = timestamp
    
for crypto in currencies:
    klines = client.get_historical_klines(crypto, timeperiod, startdate)
    
    for line in klines:
        del line[6:]
    
    df[crypto] = pd.DataFrame(klines, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    df[crypto]['date'] = pd.to_datetime(df[crypto]['date'], unit='ms')
    df[crypto].set_index('date', inplace=True)
    
    df[crypto] = df[crypto].astype(float)

for key in df:
    df[key].to_csv('hourly' + key + '.csv')

df_hourly = {}
path = "/Users/Hamid/Documents/hourly/"
for key in os.listdir(path):
    print(key[:-4]) # the weird slicing is to drop the .csv extension
    df_hourly[key[:-4]] = pd.read_csv('hourly/' + key)
    df_hourly[key[:-4]]['date'] = pd.to_datetime(df_hourly[key[:-4]]['date'])# ,unit='ms')
    df_hourly[key[:-4]].set_index('date', inplace=True)

#create datafeed class for pandas dataframes
class PandasData(btfeeds.feed.DataBase):
    '''
    The ``dataname`` parameter inherited from ``feed.DataBase`` is the pandas
    DataFrame
    '''

    params = (
        # Possible values for datetime (must always be present)
        #  None : datetime is the "index" in the Pandas Dataframe
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ('datetime', None),

        # Possible values below:
        #  None : column not present
        #  -1 : autodetect position or case-wise equal name
        #  >= 0 : numeric index to the colum in the pandas dataframe
        #  string : column name (as index) in the pandas dataframe
        ('open', -1),
        ('high', -1),
        ('low', -1),
        ('close', -1),
        ('volume', -1),
        ('openinterest', -1))

class PairTradingStrategy(bt.Strategy):
    params = dict (
        period=10,
        stake=10,
        qty1=0,
        qty2=0,
        printout=True,
        upper=2.1,
        lower=-2.1,
        up_medium=0.5,
        low_medium=-0.5,
        status=0,
        portfolio_value=100000,
        name1='none',
        name2='none')

    def start(self):
        #record results
        if not os.path.exists('mystats_hour.csv'):
            self.mystats = open('mystats_hour.csv', 'w')
            self.mystats.write('date,pair1,pair2,portfolio,period,status,zscore,pair1_pos,pair2_pos,order_flag\n')
        else:
            self.mystats = open('mystats_hour.csv', 'a')
            
        if not os.path.exists('endstats_hour.csv'):
            self.mystats2 = open('endstats_hour.csv', 'w')
            self.mystats2.write('pair1,pair2,portfolio,period\n')
        else:
            self.mystats2 = open('endstats_hour.csv', 'a')

        
    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                buytxt = 'BUY COMPLETE, %.2f' % order.executed.price
                self.log(buytxt, order.executed.dt)
            else:
                selltxt = 'SELL COMPLETE, %.2f' % order.executed.price
                self.log(selltxt, order.executed.dt)

        elif order.status in [order.Expired, order.Canceled, order.Margin]:
            self.log('%s ,' % order.Status[order.status])
            pass  # Simply log

        # Allow new orders
        self.orderid = None

    def __init__(self):
        # To control operation entries
        self.orderid = None
        self.qty1 = self.p.qty1
        self.qty2 = self.p.qty2
        self.upper_limit = self.p.upper
        self.lower_limit = self.p.lower
        self.up_medium = self.p.up_medium
        self.low_medium = self.p.low_medium
        self.status = self.p.status
        self.portfolio_value = self.p.portfolio_value
        self.pair1 = self.p.name1
        self.pair2 = self.p.name2

        # Signals performed with PD.OLS :
        self.transform = btind.OLS_TransformationN(self.data0, self.data1,
                                                   period=self.p.period)
        self.zscore = self.transform.zscore

    def next(self):

        if self.orderid:
            self.mystats.write(self.data.datetime.datetime(0).strftime('%Y-%m-%dT%H:%M:%S'))
            self.mystats.write(',{}'.format(self.pair1))
            self.mystats.write(',{}'.format(self.pair2))
            self.mystats.write(',{}'.format(self.broker.getvalue()))
            self.mystats.write(',{}'.format(self.p.period))
            self.mystats.write(',{}'.format(self.status))
            self.mystats.write(',{}'.format(self.zscore[0]))
            self.mystats.write(',{}'.format(self.getposition(self.data0).size))
            self.mystats.write(',{}'.format(self.getposition(self.data1).size))
            self.mystats.write(',{}'.format(True))
            self.mystats.write('\n')
            return  # if an order is active, no new orders are allowed

        # Step 2: Check conditions for SHORT & place the order
        # Checking the condition for SHORT
        if (self.zscore[0] > self.upper_limit) and (self.status != 1):

            # Calculating the number of shares for each stock
            value = 0.5 * self.portfolio_value  # Divide the cash equally
            x = int(value / (self.data0.close))  # Find the number of shares for Stock1
            y = int(value / (self.data1.close))  # Find the number of shares for Stock2
            print('x + self.qty1 is', x + self.qty1)
            print('y + self.qty2 is', y + self.qty2)

            # Placing the order
            self.log('SELL CREATE %s, price = %.2f, qty = %d' % (self.p.name1, self.data0.close[0], x + self.qty1))
            self.sell(data=self.data0, size=(x + self.qty1))  # Place an order for buying y + qty2 shares
            self.log('BUY CREATE %s, price = %.2f, qty = %d' % (self.p.name2, self.data1.close[0], y + self.qty2))
            self.buy(data=self.data1, size=(y + self.qty2))  # Place an order for selling x + qty1 shares

            # Updating the counters with new value
            self.qty1 = x  # The new open position quantity for Stock1 is x shares
            self.qty2 = y  # The new open position quantity for Stock2 is y shares

            self.status = 1  # The current status is "short the spread"

            # Step 3: Check conditions for LONG & place the order
            # Checking the condition for LONG
        elif (self.zscore[0] < self.lower_limit) and (self.status != 2):
            # Calculating the number of shares for each stock
            value = 0.5 * self.portfolio_value  # Divide the cash equally
            x = int(value / (self.data0.close))  # Find the number of shares for Stock1
            y = int(value / (self.data1.close))  # Find the number of shares for Stock2
            print('x + self.qty1 is', x + self.qty1)
            print('y + self.qty2 is', y + self.qty2)

            # Place the order
            self.log('BUY CREATE %s, price = %.2f, qty = %d' % (self.p.name1, self.data0.close[0], x + self.qty1))
            self.buy(data=self.data0, size=(x + self.qty1))  # Place an order for buying x + qty1 shares
            self.log('SELL CREATE %s, price = %.2f, qty = %d' % (self.p.name2, self.data1.close[0], y + self.qty2))
            self.sell(data=self.data1, size=(y + self.qty2))  # Place an order for selling y + qty2 shares

            # Updating the counters with new value
            self.qty1 = x  # The new open position quantity for Stock1 is x shares
            self.qty2 = y  # The new open position quantity for Stock2 is y shares
            self.status = 2  # The current status is "long the spread"


        # Step 4: Check conditions for No Trade
        # If the z-score is within the two bounds, close all
        
        elif (self.zscore[0] < self.up_medium and self.zscore[0] > self.low_medium and self.status != 0):
            self.log('CLOSE Position %s, price = %.2f, qty = %d' % (self.p.name1, self.data0.close[0],self.getposition(self.data0).size))
            self.close(self.data0)
            self.log('CLOSE Position %s, price = %.2f, qty = %d' % (self.p.name2, self.data1.close[0],self.getposition(self.data1).size))
            self.close(self.data1)
            self.status = 0
            
        self.mystats.write(self.data.datetime.datetime(0).strftime('%Y-%m-%dT%H:%M:%S'))
        self.mystats.write(',{}'.format(self.pair1))
        
        self.mystats.write(',{}'.format(self.pair2))
        self.mystats.write(',{}'.format(self.broker.getvalue()))
        self.mystats.write(',{}'.format(self.p.period))
        self.mystats.write(',{}'.format(self.status))
        self.mystats.write(',{}'.format(self.zscore[0]))
        self.mystats.write(',{}'.format(self.getposition(self.data0).size))
        self.mystats.write(',{}'.format(self.getposition(self.data1).size))
        self.mystats.write(',{}'.format(False))
        self.mystats.write('\n')
        

    def stop(self):
        print('==================================================')
        print('Starting Value - %.2f' % self.broker.startingcash)
        print('Ending   Value - %.2f' % self.broker.getvalue())
        print('==================================================')
        self.mystats2.write('{}'.format(self.pair1))
        
        self.mystats2.write(',{}'.format(self.pair2))
        self.mystats2.write(',{}'.format(self.broker.getvalue()))
        self.mystats2.write(',{}'.format(self.p.period))
        self.mystats2.write('\n')
        self.mystats.close()
        self.mystats2.close()


def runstrategy(name1,name2,period,cash):

    # Create a cerebro
    cerebro = bt.Cerebro()

    # Create the 1st data
    data0 = bt.feeds.PandasData(dataname=df[name1])

    # Add the 1st data to cerebro
    cerebro.adddata(data0)

    # Create the 2nd data
    data1 = bt.feeds.PandasData(dataname=df[name2])

    # Add the 2nd data to cerebro
    cerebro.adddata(data1)

    # Add the strategy
    cerebro.addstrategy(PairTradingStrategy,
                        period=period,
                        stake=10,name1=name1,name2=name2,status=0,portfolio_value=cash)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcash(cash)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcommission(commission=0.0)

    # And run it
    cerebro.run()
    
idx = 1
while (idx < len(currencies)):
    runstrategy('BTCUSDT', currencies[idx], 264, 100000)
    idx += 1
