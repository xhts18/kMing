from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse
import logging
import  requests

import json
# Create your views here.

logger = logging.getLogger('django')
domain = "http://localhost:4242"
query_url = "/api/query/?detail"

@csrf_exempt
def index(request):
    # if request.method =='POST':

    url = domain + query_url
    bodyStr = request.body.decode("utf-8")
    logger.info(bodyStr)
    dStr = json.loads(bodyStr)
    response = requests.post(url, json=dStr)
    logger.info(response.content)
    contentStr = response.content.decode()
    return HttpResponse(content=contentStr)
