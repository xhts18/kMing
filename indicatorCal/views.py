from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import logging
import datetime
from .dataNetSource import JoinQuant
from .tsdbClient import  TsdbClient
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
import json
from .utils import Utils
from .bollingBand import BollingBand
from .mfi import  MFI
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



def myschedule():
    global  startTime
    global  joinQuant


    endTime =  startTime + datetime.timedelta(minutes=1)
    startTimeStr = startTime.strftime(datetimeFormat)
    endTimeStr = endTime.strftime(datetimeFormat)


    # now = datetime.datetime.now()
    # fillSecondTime = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)
    # endTime = fillSecondTime + datetime.timedelta(minutes=-1)
    # startTime = fillSecondTime + datetime.timedelta(minutes=-2)

    startTimeStr = startTime.strftime(datetimeFormat)
    endTimeStr = endTime.strftime(datetimeFormat)


    logger.info("startTime : "+startTimeStr+" endTime : "+endTimeStr)

    for security in securityList:
        facade(security, startTimeStr, endTimeStr, endTime.timestamp() * 1000)


    startTime = endTime




def facade(security,startTimeStr,endTimeStr,currentTimestamp):

    dataFrame = joinQuant.getLastMinuteDataFrame(security,startTimeStr,endTimeStr)


    if len(dataFrame) != 1 and len(dataFrame) != 2:
        logger.info("startTime : " + startTimeStr + " endTime : " + endTimeStr + " dataFrame len is not 1 or 2")
        return

    close = dataFrame.iloc[-1]['close']
    high = dataFrame.iloc[-1]['high']
    low = dataFrame.iloc[-1]['low']
    volume = dataFrame.iloc[-1]['volume']


    logger.info("startTime : " + startTimeStr + " endTime : " + endTimeStr +" security : "+security+  " close : " + str(close)+" high : "+str(high)+" low : "+str(low)+" volume : "+str(volume))

    global last_15min_close_price_list
    global last_15min_high_price_list
    global last_15min_low_price_list
    global last_15min_volume_list

    last_15min_close_price_list.append(close)
    last_15min_high_price_list.append(high)
    last_15min_low_price_list.append(low)
    last_15min_volume_list.append(volume)

    closePricePutJson =  utils.buildPutJson(real_price_metric,currentTimestamp, close, security)
    tsdbClient.put(closePricePutJson)

    # 布林带和mfi
    if len(last_15min_close_price_list) >=15:
        bollingBand.calReal(currentTimestamp, last_15min_close_price_list, security)
        mfi.calReal(last_15min_high_price_list,last_15min_low_price_list,last_15min_close_price_list,last_15min_volume_list,currentTimestamp,security)

        last_15min_close_price_list.pop(0)
        last_15min_high_price_list.pop(0)
        last_15min_low_price_list.pop(0)
        last_15min_volume_list.pop(0)
    else:
        mfi.mock(currentTimestamp,security)


# 初始化变量

# 日期
datetimeFormat= '%Y-%m-%d %H:%M:%S'
startTime = datetime.datetime.strptime('2018-07-24 09:00:00',datetimeFormat)

# 日志
logger = logging.getLogger('django')


# 数据源客户端初始化
# securityList = ['M1901.XDCE','SR9999.XZCE','RB9999.XSGE']
securityList = ['SR9999.XZCE']
# 内存list
last_15min_close_price_list = []
last_15min_high_price_list = []
last_15min_low_price_list = []
last_15min_volume_list = []




# metric
real_price_metric = "joinQuant.futures.price" # 数据源 + 证券类型 + 指标类型

joinQuant = JoinQuant()
utils = Utils()
tsdbClient = TsdbClient("http://localhost:4242")

bollingBand = BollingBand()
mfi = MFI()
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


scheduler = BackgroundScheduler()
scheduler.configure(job_defaults=job_defaults,executors=executors)
scheduler.add_job(myschedule, 'interval', seconds=15)
# scheduler.start()