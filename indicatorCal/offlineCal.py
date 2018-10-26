from .dataNetSource import JoinQuant
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import datetime
import logging
import talib

from .tsdbClient import  TsdbClient
from .utils import Utils

from .mfi import MFI
from .bollingBand import BollingBand

executors = {
    'default': {'type': 'threadpool', 'max_workers': 1},
    'processpool': ProcessPoolExecutor(max_workers = 1)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 10
}


def myschedule():
    logger.info("begin offline schedule")

    costStart = datetime.datetime.now()

    sugarPriceDataFrame = joinQuant.getHistoryPriceDf(sugar, startTimeStr, endTimeStr)
    costEnd = datetime.datetime.now()

    historyCost = (costEnd.timestamp() - costStart.timestamp()) * 1000
    logger.info(" getHistory cost " + str(historyCost))


    row = sugarPriceDataFrame.shape[0]
    logger.info("row: "+str(row))

    for i in range(1,row+1):
        sugarPriceDf = sugarPriceDataFrame.iloc[i-1:i]

        currentDateIndex = sugarPriceDf.index

        dateStr = currentDateIndex.strftime(datetimeFormat)[0]
        dateTime = datetime.datetime.strptime(dateStr, datetimeFormat)

        currentTimestamp = dateTime.timestamp() * 1000

        sugarPrice = sugarPriceDf['close'].values[0]

        if  not sugarPrice:
            continue
        sugarPutJson = utils.buildPutJson(real_price_metric, currentTimestamp, sugarPrice, sugar)
        tsdbClient.put(sugarPutJson)



        # mfi 计算
        if i < 15:
            mfi.mock(currentTimestamp,sugar)
        else:
            mfi.cal(sugarPriceDataFrame,i,currentTimestamp,sugar)

        # 布林带计算
        if i >= 14:
            bollingBand.cal(sugarPriceDataFrame,i,currentTimestamp,sugar)
        else:
            bollingBand.mock(currentTimestamp,sugar)

    costEnd = datetime.datetime.now()

    allCost = (costEnd.timestamp() - costStart.timestamp()) * 1000
    logger.info("all Finish cost " + str(allCost))

def task():
    global endTimeStr
    global startTimeStr
    global datetimeFormat

    for i in range(0,31):
        logger.info("now startTime " + startTimeStr + " endTime : " + endTimeStr)

        startTime = datetime.datetime.strptime(startTimeStr, datetimeFormat)
        endTime = datetime.datetime.strptime(endTimeStr, datetimeFormat)

        nextstartTime = startTime + datetime.timedelta(days=1)
        nextEndTime = endTime + datetime.timedelta(days=1)

        startTimeStr = nextstartTime.strftime(datetimeFormat)
        endTimeStr = nextEndTime.strftime(datetimeFormat)

        myschedule()





utils = Utils()
joinQuant = JoinQuant()
tsdbClient = TsdbClient('http://localhost:4242')
logger = logging.getLogger('django')

# security = 'M1901.XDCE'
sugar = 'SR9999.XZCE'
# soyBean = 'Y9999.XDCE'
# palm = 'P9999.XDCE'




real_price_metric = "joinQuant.futures.price" # 数据源 + 证券类型 + 指标类型

datetimeFormat= '%Y-%m-%d %H:%M:%S'
startTimeStr = '2018-06-30 09:00:00'
endTimeStr = '2018-06-30 15:00:00'


mfi = MFI()
bollingBand = BollingBand()

scheduler = BackgroundScheduler()
scheduler.configure(job_defaults=job_defaults,executors=executors)
scheduler.add_job(task, 'interval', seconds=300)
# scheduler.start()



















# macd = MACD()
# dma = DMA()
# bollingBand = BollingBand()
# trix = Trix()
# ema = EMA()
# adx = ADX()



# 布林带
# if i < 30:
#     bollingBand.mock(currentTimestamp,security)
# elif i >= 30:
#     bollingBand.cal(priceDateFrame,i,currentTimestamp,security)

# ADX计算
# if i < 30:
#     adx.mock(currentTimestamp,security)
# elif i >= 30:
#     adx.cal(priceDateFrame,i,currentTimestamp,security)
#     pass

# EMA计算
# if i >=60:
#     ema.cal(priceDateFrame,i,currentTimestamp,security)

# MACD计算
# if i < 30:
#     macd.mock(currentTimestamp,security)
# if i >= 30:
#     macd.cal(priceDateFrame,i,currentTimestamp,security)

# DMA计算
# if i < 20:
#     dma.mockDma(currentTimestamp,security)
# elif 20 <= i and i < 30:
#     dma.mockAma(currentTimestamp,security)
#     dma.calDma(priceDateFrame,i,currentTimestamp,security)
# elif i >= 30:
#     dma.calDma(priceDateFrame,i,currentTimestamp,security)
#     dma.calAma(priceDateFrame, i, currentTimestamp, security)

# Trix计算
# if i < 30:
#     trix.mock(currentTimestamp,security)
# else:
#     trix.cal(priceDateFrame,i,currentTimestamp,security)