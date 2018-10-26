import logging
import talib
from .tsdbClient import TsdbClient
from .utils import  Utils

class ADX:
    def cal(self,dataFrame,index,currentTimestamp,security):

        dataHigh = dataFrame.iloc[index - data_frame_period : index]['high'].values
        dataLow = dataFrame.iloc[index - data_frame_period : index]['low'].values
        dataClose = dataFrame.iloc[index - data_frame_period: index]['close'].values

        adxNdArray = talib.ADX(dataHigh,dataLow,dataClose,timeperiod=adx_period)
        adxValue = round(adxNdArray[-1],2)
        adxJson = utils.buildPutJson(adx_metric, currentTimestamp, adxValue, security)

        tsdbClient.put(adxJson)

    def mock(self, currentTimestamp, security):
        adxJson = utils.buildPutJson(adx_metric, currentTimestamp, 0.0, security)
        tsdbClient.put(adxJson)


data_frame_period = 30
adx_period = 15
adx_metric = 'indicator.adx'




tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

logger = logging.getLogger('django')