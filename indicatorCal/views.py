from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
import logging
import datetime
import jqdatasdk
from  .dataNetSource import JoinQuant
from .tsdbClient import  TsdbClient

# Create your views here.

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

    logger.info("startTime : " + startTimeStr + " endTime : " + endTimeStr + " price : " + str(price))

    last_30min_price_list.append(price)


#   写入tsdb
    putJson =  buildPutJson(real_price_metric,endTime.timestamp()*1000,price,security)
    logger.info("putJson "+ str(putJson))
    tsdbClient.put('/api/put/?details',putJson)

# 初始化变量

# 日期
datetimeFormat= '%Y-%m-%d %H:%M:%S'
startTime = datetime.datetime.strptime('2018-09-21 09:00:00',datetimeFormat)


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

# 任务
executors = {
    'default': {'type': 'threadpool', 'max_workers': 1},
    'processpool': ProcessPoolExecutor(max_workers = 1)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 1
}

scheduler = BackgroundScheduler()
scheduler.configure(job_defaults=job_defaults,executors=executors)
scheduler.add_job(myschedule, 'interval', seconds=5)
scheduler.start()