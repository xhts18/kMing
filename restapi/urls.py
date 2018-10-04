from django.urls import path
from . import views

urlpatterns = [
   path('tsdb/query', views.index, name='index')
]


