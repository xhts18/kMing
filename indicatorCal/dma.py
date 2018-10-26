import logging
import talib
import numpy as np
from .tsdbClient import TsdbClient
from .utils import  Utils

class DMA:
    def calDma(self,dataFrame,index,currentTimestamp,security):
        shortNdArray = dataFrame.iloc[index - dma_period_short:index]['close'].values
        shortMAValue = talib.MA(shortNdArray,timeperiod=dma_period_short, matype=0)[-1]

        longNdArray = dataFrame.iloc[index - dma_period_long:index]['close'].values
        longMAValue = talib.MA(longNdArray,timeperiod=dma_period_long,matype=0)[-1]

        dmaValue = round(shortMAValue - longMAValue,2)

        dmaJson = utils.buildPutJson(dma_metric, currentTimestamp, dmaValue, security)
        tsdbClient.put(dmaJson)

        dma_list_10min.append(dmaValue)

        if(len(dma_list_10min) > 10):
            dma_list_10min.pop(0)


    def calAma(self,dataFrame,index,currentTimestamp,security):


        amaValue = round(talib.MA(np.array(dma_list_10min),timeperiod=ama_period, matype=0)[-1],2)

        amaJson = utils.buildPutJson(ama_metric, currentTimestamp, amaValue, security)
        tsdbClient.put(amaJson)

        if (len(dma_list_10min) > 10):
            dma_list_10min.pop(0)

    def mockDma(self,currentTimestamp,security):
        macdJson = utils.buildPutJson(dma_metric, currentTimestamp, 0.0, security)
        tsdbClient.put(macdJson)

    def mockAma(self, currentTimestamp, security):
        macdJson = utils.buildPutJson(ama_metric, currentTimestamp, 0.0, security)
        tsdbClient.put(macdJson)


dma_metric = "indicator.dma"
ama_metric = "indicator.ama"

dma_period_short = 10
dma_period_long = 20
ama_period = 10

dma_list_10min = []



tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

logger = logging.getLogger('django')