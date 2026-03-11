from __future__ import annotations

from django.contrib import admin
from django.views.generic import RedirectView
from django.urls import include, path


urlpatterns = [
    path("admin/", admin.site.urls),
    path("", RedirectView.as_view(pattern_name="index", permanent=False)),
    path("", include("apps.dashboard.urls")),
    
    path("pipeline/", include("apps.pipeline.urls")),
    path("sales/", include("apps.sales.urls")),
]
