from django.shortcuts import render
from apscheduler.schedulers.background import BackgroundScheduler


# Create your views here.


def index():
    pass


def myfunc():
   print("doing job")

scheduler = BackgroundScheduler()
scheduler.add_job(myfunc, 'interval', seconds=10)
scheduler.start()