from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
import logging
import requests

import json
# Create your views here.

logger = logging.getLogger('django')
domain = "http://localhost:4242"
query_url = "/api/query/?detail"

@csrf_exempt
def index(request):
    url = domain + query_url

    # 处理request 构造参数
    bodyStr = request.body.decode("utf-8")
    dictRequestBody = json.loads(bodyStr)
    start = dictRequestBody["start"]
    end = dictRequestBody["end"]
    security = dictRequestBody["security"]
    # response的结构
    responseDataList = []


    queryParam = buildParam(start,end,real_price_metric,security)

    # price
    realPriceResponse = requests.post(url, json=queryParam)
    realPriceDict = responseToJson(realPriceResponse)

    initBuildResponse(responseDataList,realPriceDict)

    #  upper metric
    queryParam = buildParam(start,end,upper_band_metric,security)
    upperbandResponse = requests.post(url, json=queryParam)
    upperbandmetric = responseToJson(upperbandResponse)
    mergeResponse(responseDataList,upperbandmetric)


    #  middle metric
    queryParam = buildParam(start, end, middle_band_metric,security)
    middlebandResponse = requests.post(url, json=queryParam)
    middlebandmetric = responseToJson(middlebandResponse)
    mergeResponse(responseDataList,middlebandmetric)

    # low metric
    queryParam = buildParam(start, end, low_band_metric,security)
    lowbandResponse = requests.post(url, json=queryParam)
    lowbandmetric = responseToJson(lowbandResponse)
    mergeResponse(responseDataList, lowbandmetric)

    responseDataList.sort(key = lambda x:x["name"])

    formatNameToFloat(responseDataList)
    return HttpResponse(content=(json.dumps(responseDataList)))

# price
def formatNameToFloat(responseDataList):

    for obj in responseDataList:
        name = obj["name"]
        obj["name"] = float(name)*1000

def responseToJson(response):

    realPriceRStr = response.content.decode()
    responseDict = json.loads(realPriceRStr)

    return responseDict


def initBuildResponse(queriesList,responseDict):

    if not responseDict:
        return
    if not responseDict[0]:
        return



    dps = responseDict[0]["dps"]
    metric = responseDict[0]["metric"]

    if not dps or not metric:
        return
    for k in dps:
        onePoint ={}
        onePoint["name"] = k
        onePoint[metric] = round(dps[k],2)
        queriesList.append(onePoint)



def buildParam(start,end,metric,security):

    queryParam = {}
    queryParam["start"] = start
    queryParam["end"] = end

    queryObj = {}
    queryObj["aggregator"] = "sum"
    queryObj["metric"] = metric

    tagsObj = {}
    tagsObj["security"] = security
    queryObj["tags"] = tagsObj

    queriesList = []
    queriesList.append(queryObj)

    queryParam["queries"] = queriesList

    return queryParam

def mergeResponse(resopnseDataList,responseDict):
    if not responseDict:
        return
    if not responseDict[0]:
        return

    dps = responseDict[0]["dps"]
    metric = responseDict[0]["metric"]

    keyset = dps.keys()
    for obj in resopnseDataList:
        name = obj["name"]
        if name in keyset:
            obj[metric] = round(dps[name],2)

@csrf_exempt
def oneMetric(request):
    url = domain + query_url

    # 处理request 构造参数
    bodyStr = request.body.decode("utf-8")
    dictRequestBody = json.loads(bodyStr)
    start = dictRequestBody["start"]
    end = dictRequestBody["end"]
    security = dictRequestBody["security"]
    metric = dictRequestBody["metric"]

    # response的结构
    responseDataList = []

    queryParam = buildParam(start, end, metric, security)

    # price
    realPriceResponse = requests.post(url, json=queryParam)
    realPriceDict = responseToJson(realPriceResponse)


    initBuildResponse(responseDataList, realPriceDict)
    responseDataList.sort(key = lambda x:x["name"])
    formatNameToFloat(responseDataList)
    return HttpResponse(content=(json.dumps(responseDataList)))



@csrf_exempt
def mulMetric(request):
    pass
    url = domain + query_url

    # 处理request 构造参数
    bodyStr = request.body.decode("utf-8")
    dictRequestBody = json.loads(bodyStr)
    start = dictRequestBody["start"]
    end = dictRequestBody["end"]
    security = dictRequestBody["security"]
    metrics = dictRequestBody["metrics"]

    responseDataList = []
    lenMetrics = len(metrics)


    for index in range(0,lenMetrics):
        metric = metrics[index]

        queryParam = buildParam(start, end, metric, security)

        metricResponse = requests.post(url, json=queryParam)
        responseDict = responseToJson(metricResponse)

        if index == 0:
            initBuildResponse(responseDataList, responseDict)
        else:
            mergeResponse(responseDataList, responseDict)

    responseDataList.sort(key = lambda x:x["name"])
    formatNameToFloat(responseDataList)

    return HttpResponse(content=(json.dumps(responseDataList)))



real_price_metric = "joinQuant.futures.price" # 数据源 + 证券类型 + 指标类型

upper_band_metric = "indicator.band.upper"
middle_band_metric = "indicator.band.middle"
low_band_metric = "indicator.band.low"

