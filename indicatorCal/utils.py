


class Utils:
    def buildPutJson(self,metric, timestamp, value, tag):
        dataDict = {}
        dataDict["metric"] = metric
        dataDict["timestamp"] = timestamp
        dataDict["value"] = value

        tagDict = {}
        tagDict["security"] = tag
        dataDict["tags"] = tagDict

        return dataDict