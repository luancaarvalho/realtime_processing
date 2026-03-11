from django.urls import path
from . import views

app_name = "pipeline"

urlpatterns = [
    path("start/", views.PipelineStartView.as_view(), name="start"),
    path("stop/", views.PipelineStopView.as_view(), name="stop"),
    path("status/", views.PipelineStatusView.as_view(), name="status"),
]
