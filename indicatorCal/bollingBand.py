import logging
import talib
from .tsdbClient import  TsdbClient
from .utils import Utils

import numpy as np

class BollingBand:

    def cal(self,priceDateFrame,index,currentTimestamp,security):

        recent_30_min = priceDateFrame[index-band_timeperiod:index]['close'].values


        upperbandNdArray,middlebandNdArray,lowerbandNdArray = talib.BBANDS(recent_30_min,
                                                                             timeperiod=band_timeperiod,
                                                                             nbdevup=2, nbdevdn=2, matype=0)
        upperband = upperbandNdArray[-1]
        middleband = middlebandNdArray[-1]
        lowband = lowerbandNdArray[-1]

        # upperLowSub = upperband - lowband

        upperbandJson = utils.buildPutJson(upper_band_metric, currentTimestamp, upperband, security)
        middlebandJson = utils.buildPutJson(middle_band_metric, currentTimestamp, middleband, security)
        lowbandJson = utils.buildPutJson(low_band_metric, currentTimestamp, lowband, security)
        # upperLowSubJson = utils.buildPutJson(upper_low_sub, currentTimestamp, upperLowSub, security)

        tsdbClient.put(upperbandJson)
        tsdbClient.put(middlebandJson)
        tsdbClient.put(lowbandJson)
        # tsdbClient.put(upperLowSubJson)

    def mock(self,currentTimestamp,security):
        upperLowSubJson = utils.buildPutJson(upper_low_sub, currentTimestamp, 0.0, security)
        tsdbClient.put(upperLowSubJson)
        pass


    def calReal(self,timestamp, last_14min_close_list, security):

        upperbandNdArray, middlebandNdArray, lowerbandNdArray = talib.BBANDS(np.array(last_14min_close_list),
                                                                             timeperiod=band_timeperiod,
                                                                             nbdevup=2, nbdevdn=2, matype=0)
        upperband = upperbandNdArray[-1]
        middleband = middlebandNdArray[-1]
        lowband = lowerbandNdArray[-1]

        upperbandJson = utils.buildPutJson(upper_band_metric, timestamp, upperband, security)
        middlebandJson = utils.buildPutJson(middle_band_metric, timestamp, middleband, security)
        lowbandJson = utils.buildPutJson(low_band_metric, timestamp, lowband, security)


        tsdbClient.put(upperbandJson)
        tsdbClient.put(middlebandJson)
        tsdbClient.put(lowbandJson)


band_timeperiod = 14

tsdbClient = TsdbClient("http://localhost:4242")
utils = Utils()

upper_band_metric = "indicator.band.upper"
middle_band_metric = "indicator.band.middle"
low_band_metric = "indicator.band.low"

upper_low_sub = 'indicator.band.upperLow.sub'

logger = logging.getLogger('django')
