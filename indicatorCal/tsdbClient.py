import requests
import logging
class TsdbClient:
    def __init__(self,domain):
        self.domain = domain
        pass

    def put(self,dataJson):

        logger.info("putJson " + str(dataJson))
        url = self.domain +'/api/put/?details'
        requests.post(url,json=dataJson)

    def query(self,suffix):
        pass


logger = logging.getLogger('django')
