import logging
import talib
from .tsdbClient import  TsdbClient
from .utils import Utils

class Trix:

    def cal(self,priceDateFrame,index,currentTimestamp,security):

        recent_30_min = priceDateFrame[index-30:index]['close'].values

        trixNdArray = talib.TRIX(recent_30_min,timeperiod=trix_period)
        trixValue = round(trixNdArray[-1],3)


        trixJson = utils.buildPutJson(trix_metric, currentTimestamp, trixValue, security)
        tsdbClient.put(trixJson)

    def mock(self,currentTimestamp,security):
        trixJson = utils.buildPutJson(trix_metric, currentTimestamp, 0.0, security)

        tsdbClient.put(trixJson)


tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

trix_metric ="indicator.trix"
trix_period = 10


logger = logging.getLogger('django')

