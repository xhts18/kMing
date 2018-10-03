import requests

class TsdbClient:
    def __init__(self,domain):
        self.domain = domain
        pass

    def put(self,suffix,dataJson):
        url = self.domain +suffix
        requests.post(url,json=dataJson)

    def query(self,suffix):
        pass


