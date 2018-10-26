import logging
import talib
from .tsdbClient import TsdbClient
from .utils import  Utils
import numpy as np
class MFI:
    def cal(self,dataFrame,index,currentTimestamp,security):

        startIndex = index - mfi_index_range
        dataHigh = dataFrame.iloc[startIndex : index]['high'].values
        dataLow = dataFrame.iloc[startIndex : index]['low'].values
        dataClose = dataFrame.iloc[startIndex : index]['close'].values
        dataVolume = dataFrame.iloc[startIndex :index]['volume'].values

        mfiArray = talib.MFI(dataHigh,dataLow,dataClose,dataVolume,timeperiod=mfi_timeperiod)
        mfiValue = mfiArray[-1]

        mfiJson = utils.buildPutJson(mfi_metric, currentTimestamp, mfiValue, security)

        tsdbClient.put(mfiJson)

    def mock(self,currentTimestamp,security):
        macdJson = utils.buildPutJson(mfi_metric, currentTimestamp, 0.0, security)
        tsdbClient.put(macdJson)

    def calReal(self,last_15min_high_price_list,last_15min_low_price_list,last_15min_close_price_list,last_15min_volume_list,currentTimestamp,security):

        dataHigh = np.array(last_15min_high_price_list)
        dataLow = np.array(last_15min_low_price_list)
        dataClose = np.array(last_15min_close_price_list)
        dataVolume = np.array(last_15min_volume_list)

        mfiArray = talib.MFI(dataHigh,dataLow,dataClose,dataVolume,timeperiod=mfi_timeperiod)
        mfiValue = mfiArray[-1]

        mfiJson = utils.buildPutJson(mfi_metric, currentTimestamp, mfiValue, security)
        tsdbClient.put(mfiJson)

mfi_index_range = 15
mfi_timeperiod = 14
mfi_metric = "indicator.mfi"


tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

logger = logging.getLogger('django')