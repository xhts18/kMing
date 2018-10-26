import logging
import talib
from .tsdbClient import TsdbClient
from .utils import  Utils

class EMA:
    def cal(self,dataFrame,index,currentTimestamp,security):

        data = dataFrame.iloc[index - ema_period : index]['close'].values

        emaNdArray = talib.EMA(data,timeperiod=ema_period)
        emaValue = round(emaNdArray[-1],2)
        emaJson = utils.buildPutJson(ema_metric, currentTimestamp, emaValue, security)

        tsdbClient.put(emaJson)

    def mock(self,currentTimestamp,security):
        emaJson = utils.buildPutJson(ema_metric, currentTimestamp, 0.0, security)

        tsdbClient.put(emaJson)


ema_period = 60
ema_metric = "indicator.ema.60"




tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

logger = logging.getLogger('django')