from django.urls import path
from . import views

urlpatterns = [
   path('tsdb/query', views.index, name='index'),
   path('tsdb/queryOneMetric',views.oneMetric,name='oneMetric'),
   path('tsdb/queryMulMetric', views.mulMetric, name='mulMetric')
]


