import logging
import talib
from .tsdbClient import TsdbClient
from .utils import  Utils

class ATR:
    def cal(self,dataFrame,index,currentTimestamp,security):

        startIndex = index - atr_index_range
        dataHigh = dataFrame.iloc[startIndex : index]['high'].values
        dataLow = dataFrame.iloc[startIndex : index]['low'].values
        dataClose = dataFrame.iloc[startIndex : index]['close'].values

        atrArray = talib.ATR(dataHigh,dataLow,dataClose,timeperiod=atr_timeperiod)
        atrValue = atrArray[-1]

        macdJson = utils.buildPutJson(mfi_metric, currentTimestamp, atrValue, security)

        tsdbClient.put(macdJson)

    def mock(self,currentTimestamp,security):
        macdJson = utils.buildPutJson(mfi_metric, currentTimestamp, 0.0, security)
        tsdbClient.put(macdJson)


atr_index_range = 15
atr_timeperiod = 14
mfi_metric = "indicator.atr"


tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

logger = logging.getLogger('django')