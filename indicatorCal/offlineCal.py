from .dataNetSource import JoinQuant
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import  numpy as np
import talib
import datetime
import logging

from .tsdbClient import  TsdbClient


joinQuant = JoinQuant()
security = 'M1901.XDCE'




real_price_metric = "joinQuant.futures.price" # 数据源 + 证券类型 + 指标类型
upper_band_metric = "indicator.band.upper"
middle_band_metric = "indicator.band.middle"
low_band_metric = "indicator.band.low"

ris_period = 29
rsi_metric = "indicator.rsi"

tsdbClient = TsdbClient('http://localhost:4242')


executors = {
    'default': {'type': 'threadpool', 'max_workers': 1},
    'processpool': ProcessPoolExecutor(max_workers = 1)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 10
}

logger = logging.getLogger('django')


def buildPutJson(metric,timestamp,value,tag):
    dataDict = {}
    dataDict["metric"] = metric
    dataDict["timestamp"] = timestamp
    dataDict["value"] = value

    tagDict = {}
    tagDict["security"] = tag
    dataDict["tags"] = tagDict

    return dataDict


def myschedule():
    logger.info("begin offline schedule")

    costStart = datetime.datetime.now()
    priceDateFrame = joinQuant.getHistoryPriceDf(security, startTimeStr, endTimeStr)
    costEnd = datetime.datetime.now()

    historyCost = (costEnd.timestamp() - costStart.timestamp()) * 1000
    logger.info(" getHistory cost " + str(historyCost))


    row = priceDateFrame.shape[0]

    for i in range(1,row+1):
        currentPriceDataFrame = priceDateFrame.iloc[i-1:i]

        currentDateIndex = currentPriceDataFrame.index
        dateStr = currentDateIndex.strftime(datetimeFormat)[0]
        dateTime = datetime.datetime.strptime(dateStr, datetimeFormat)

        currentTimestamp = dateTime.timestamp() * 1000
        currentPrice = currentPriceDataFrame['close'].values[0]

        putJson = buildPutJson(real_price_metric, currentTimestamp, currentPrice, security)
        tsdbClient.put(putJson)



        if i < 30:
            # 写入ris
            rsiJson = buildPutJson(rsi_metric, currentTimestamp, 0.0, security)
            tsdbClient.put(rsiJson)
            continue

        recent_30_min = priceDateFrame[i-30:i]['close'].values


        upperbandNdArray, middlebandNdArray, lowerbandNdArray = talib.BBANDS(recent_30_min,
                                                                             timeperiod=30,
                                                                             nbdevup=2, nbdevdn=2, matype=0)
        upperband = upperbandNdArray[-1]
        middleband = middlebandNdArray[-1]
        lowband = lowerbandNdArray[-1]

        upperbandJson = buildPutJson(upper_band_metric, currentTimestamp, upperband, security)
        middlebandJson = buildPutJson(middle_band_metric, currentTimestamp, middleband, security)
        lowbandJson = buildPutJson(low_band_metric, currentTimestamp, lowband, security)

        tsdbClient.put(upperbandJson)
        tsdbClient.put(middlebandJson)
        tsdbClient.put(lowbandJson)

    #   RSI指标
        rsi = round(talib.RSI(recent_30_min,ris_period)[-1],2)
        rsiJson = buildPutJson(rsi_metric,currentTimestamp,rsi,security)
        tsdbClient.put(rsiJson)


    costEnd = datetime.datetime.now()


    allCost = (costEnd.timestamp() - costStart.timestamp()) * 1000
    logger.info("all Finish cost " + str(allCost))

datetimeFormat= '%Y-%m-%d %H:%M:%S'
startTimeStr = '2018-09-27 09:00:00'
endTimeStr = '2018-09-27 15:00:00'

scheduler = BackgroundScheduler()
scheduler.configure(job_defaults=job_defaults,executors=executors)
scheduler.add_job(myschedule, 'interval', seconds=15)
# scheduler.start()