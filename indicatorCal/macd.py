import logging
import talib
from .tsdbClient import TsdbClient
from .utils import  Utils

class MACD:
    def cal(self,dataFrame,index,currentTimestamp,security):

        data = dataFrame.iloc[index - macd_period : index]['close'].values

        macdNdArray, macdsignalNdArray, macdhistNdArray = talib.MACD(data, fastperiod=10, slowperiod=25, signalperiod=6)
        macdValue = round(macdNdArray[-1],2)
        macdJson = utils.buildPutJson(macd_metric, currentTimestamp, macdValue, security)

        tsdbClient.put(macdJson)

    def mock(self,currentTimestamp,security):
        macdJson = utils.buildPutJson(macd_metric, currentTimestamp, 0.0, security)

        tsdbClient.put(macdJson)


macd_period = 30
macd_metric = "indicator.macd"

fastperiod=12
slowperiod=26
signalperiod=9



tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

logger = logging.getLogger('django')