from django.urls import path
from . import views



urlpatterns = [
   path('', views.index, name='index'),
   path('monitor/alert',views.getAlert,name='getAlert')

]


