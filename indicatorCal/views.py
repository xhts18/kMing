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
    return HttpResponse(content=(json.dumps(alert)))


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
    global  startTime
    global  joinQuant


    endTime =  startTime + datetime.timedelta(minutes=1)
    startTimeStr = startTime.strftime(datetimeFormat)
    endTimeStr = endTime.strftime(datetimeFormat)

    price = joinQuant.getLastMinutePrice(security,startTimeStr,endTimeStr)
    startTime = endTime

    if not price:
        logger.info("startTime : " + startTimeStr + " endTime : " + endTimeStr + " price is None")
        return

    logger.info("startTime : " + startTimeStr + " endTime : " + endTimeStr + " price : " + str(price))

    last_30min_price_list.append(price)


    currentTimestamp = endTime.timestamp()*1000
#   写入tsdb
    putJson = buildPutJson(real_price_metric,currentTimestamp,price,security)
    tsdbClient.put('/api/put/?details',putJson)

    if len(last_30min_price_list) < 30:
        return

#   布林带计算
    logger.info("last_30min_price_list size "+ str(len(last_30min_price_list))+"startTime : " + startTimeStr + " endTime : " + endTimeStr)
    bandCal(currentTimestamp,price)
    last_30min_price_list.pop(0)


def bandCal(timestamp,currentPrice):

    upperbandNdArray, middlebandNdArray, lowerbandNdArray = talib.BBANDS(np.array(last_30min_price_list), timeperiod=30,
                                                                         nbdevup=2, nbdevdn=2, matype=0)
    upperband = upperbandNdArray[-1]
    middleband = middlebandNdArray[-1]
    lowband = lowerbandNdArray[-1]

    upperbandJson = buildPutJson(upper_band_metric,timestamp,upperband,security)
    middlebandJson = buildPutJson(middle_band_metric,timestamp,middleband,security)
    lowbandJson = buildPutJson(low_band_metric,timestamp,lowband,security)

    tsdbClient.put('/api/put/?details',upperbandJson)
    tsdbClient.put('/api/put/?details',middlebandJson)
    tsdbClient.put('/api/put/?details',lowbandJson)

    global alert

    if currentPrice >= upperband or currentPrice <= lowband:
        alert["monitor"] = True
    else:
        alert["monitor"] = False


# 初始化变量

# 日期
datetimeFormat= '%Y-%m-%d %H:%M:%S'
startTime = datetime.datetime.strptime('2018-09-19 09:00:00',datetimeFormat)


# 日志
logger = logging.getLogger('django')


# 数据源客户端初始化
joinQuant = JoinQuant()
security = 'M1901.XDCE'


# 内存list
last_30min_price_list = []

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
    'max_instances': 10
}

alert = {}
alert["monitor"] = False


scheduler = BackgroundScheduler()
scheduler.configure(job_defaults=job_defaults,executors=executors)
scheduler.add_job(myschedule, 'interval', seconds=10)
scheduler.start()