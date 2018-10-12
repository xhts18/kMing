from django.urls import path
from . import views
from . import offlineCal


urlpatterns = [
   path('', views.index, name='index'),
   path('monitor/alert',views.getAlert,name='getAlert'),
   path('offline/',offlineCal.myschedule,name="offlineSchedule")
]


