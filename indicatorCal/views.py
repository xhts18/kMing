from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import logging
import datetime
from .dataNetSource import JoinQuant
from .tsdbClient import  TsdbClient
import  numpy as np
import talib
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
import json


# Create your views here.
@csrf_exempt
def getAlert(request):
    global alert
    for security in securityList:
        if alert[security]:
            return HttpResponse(content=(json.dumps({"monitor":True})))

    return HttpResponse(content=(json.dumps({"monitor":False})))


def index():
    pass

# (1) 计算startTime,endTime
# (2) 拿到当前最新一笔数据
# (3) 存入内存list(维持最近的30个点)
# (4) 存入tsdb
# (5) 计算新指标
#     )1 从内存list最近的30个点
#     )2 若1没有数据,则从tsdb取最近的30个点
#     )3 计算新指标
#     )4 新指标存入tsdb

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
    # global  startTime
    global  joinQuant


    # endTime =  startTime + datetime.timedelta(minutes=1)
    # startTimeStr = startTime.strftime(datetimeFormat)
    # endTimeStr = endTime.strftime(datetimeFormat)

    now = datetime.datetime.now()
    fillSecondTime = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)
    endTime = fillSecondTime + datetime.timedelta(minutes=-1)
    startTime = fillSecondTime + datetime.timedelta(minutes=-2)

    startTimeStr = startTime.strftime(datetimeFormat)
    endTimeStr = endTime.strftime(datetimeFormat)


    logger.info("startTime : "+startTimeStr+" endTime : "+endTimeStr)

    for security in securityList:
        facade(security, startTimeStr, endTimeStr, endTime.timestamp() * 1000)


    startTime = endTime


    logger.info(last_30min_price_list_dist)


def facade(security,startTimeStr,endTimeStr,currentTimestamp):

    price = joinQuant.getLastMinutePrice(security,startTimeStr,endTimeStr)
    if not price:
        logger.info("startTime : " + startTimeStr + " endTime : " + endTimeStr + " price is None")
        return

    logger.info("startTime : " + startTimeStr + " endTime : " + endTimeStr + " price : " + str(price) +" security : "+security )


    last_30min_price_list = last_30min_price_list_dist[security]
    last_30min_price_list.append(price)


    #   写入tsdb
    putJson = buildPutJson(real_price_metric, currentTimestamp, price, security)
    tsdbClient.put('/api/put/?details', putJson)

    if len(last_30min_price_list) < 30:
        return

    #   布林带计算
    bandCal(currentTimestamp, price,last_30min_price_list,security)
    last_30min_price_list.pop(0)



def bandCal(timestamp,currentPrice,last_30min_price_list,security):

    upperbandNdArray, middlebandNdArray, lowerbandNdArray = talib.BBANDS(np.array(last_30min_price_list), timeperiod=30,
                                                                         nbdevup=2, nbdevdn=2, matype=0)
    upperband = upperbandNdArray[-1]
    middleband = middlebandNdArray[-1]
    lowband = lowerbandNdArray[-1]

    upperbandJson = buildPutJson(upper_band_metric,timestamp,upperband,security)
    middlebandJson = buildPutJson(middle_band_metric,timestamp,middleband,security)
    lowbandJson = buildPutJson(low_band_metric,timestamp,lowband,security)

    tsdbClient.put(upperbandJson)
    tsdbClient.put(middlebandJson)
    tsdbClient.put(lowbandJson)

    global alert


    if currentPrice >= upperband or currentPrice <= lowband:
        alert[security] = True
    else:
        alert[security] = False


# 初始化变量

# 日期
datetimeFormat= '%Y-%m-%d %H:%M:%S'
startTime = datetime.datetime.strptime('2018-09-19 09:00:00',datetimeFormat)


# 日志
logger = logging.getLogger('django')


# 数据源客户端初始化
# joinQuant = JoinQuant()
# securityList = ['M1901.XDCE','SR9999.XZCE','RB9999.XSGE']
securityList = ['M1901.XDCE']

# 内存list
last_30min_price_list_dist = {}
for security in securityList:
    last_30min_price_list_dist[security] = []

#  tsdb客户端
tsdbClient = TsdbClient('http://localhost:4242')
real_price_metric = "joinQuant.futures.price" # 数据源 + 证券类型 + 指标类型
upper_band_metric = "indicator.band.upper"
middle_band_metric = "indicator.band.middle"
low_band_metric = "indicator.band.low"

# 任务
executors = {
    'default': {'type': 'threadpool', 'max_workers': 1},
    'processpool': ProcessPoolExecutor(max_workers = 1)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 100
}

alert = {}
for security in securityList:
    alert[security] = False


# scheduler = BackgroundScheduler()
# scheduler.configure(job_defaults=job_defaults,executors=executors)
# scheduler.add_job(myschedule, 'interval', seconds=60)
# scheduler.start()