from django.conf.urls import url, include
from django.contrib import admin

from .views import *

urlpatterns = [
    url(r'^CouchSqlCompare', CouchSqlCompare),
    url(r'^TxtExport', TxtExport),
    url(r'^generate_report', BasicReport.as_view()),
    url(r'^(?P<path>.*)$', CustomProxyView.as_view()),
]
