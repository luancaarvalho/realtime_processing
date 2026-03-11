from django.urls import path
from . import views

app_name = "sales"

urlpatterns = [
    path("kpis/", views.DashboardKpisApiView.as_view(), name="kpis"),
    path("rankings/sellers/", views.SellerRankingApiView.as_view(), name="seller-rankings"),
    path("rankings/stores/", views.StoreRankingApiView.as_view(), name="store-rankings"),
    path("profits/categories/", views.CategoryProfitApiView.as_view(), name="category-profits"),
    path("profits/channels/", views.ChannelProfitApiView.as_view(), name="channel-profits"),
    path("ingest/orders/", views.OrderIngestionApiView.as_view(), name="sales-ingest-order"),
]
