import logging
import  jqdatasdk

class JoinQuant:

    frequency_1m = '1m'
    logger = logging.getLogger('django')

    def __init__(self):
        jqdatasdk.auth('15869184964', 'shWBHB111')

    def getLastMinutePrice(self,security,startTime,endTime):
        df = jqdatasdk.get_price(security,start_date=startTime,end_date=endTime,frequency=JoinQuant.frequency_1m)
        row = df.shape[0]
        if row == 0:
            JoinQuant.logger.info("rows num is 0 , startTime : " + startTime + " endTime : "+endTime)
            return None
        price = df.iloc[-1]["close"]
        return price
