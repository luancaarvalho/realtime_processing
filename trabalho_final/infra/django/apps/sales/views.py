import json

from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from apps.sales.selectors.kpis import (
    get_dashboard_kpis,
    get_seller_rankings,
    get_store_rankings,
    get_category_profits,
    get_channel_profits,
)
from apps.sales.services import OrderIngestionService


class DashboardKpisApiView(View):
    def get(self, request, *args, **kwargs):
        window = request.GET.get("window", "15m")
        data = get_dashboard_kpis(window=window)
        return JsonResponse(data)


class SellerRankingApiView(View):
    def get(self, request, *args, **kwargs):
        limit = int(request.GET.get("limit", 5))
        window = request.GET.get("window", "15m")
        data = get_seller_rankings(limit=limit, window=window)
        return JsonResponse({"results": data})


class StoreRankingApiView(View):
    def get(self, request, *args, **kwargs):
        limit = int(request.GET.get("limit", 5))
        window = request.GET.get("window", "15m")
        data = get_store_rankings(limit=limit, window=window)
        return JsonResponse({"results": data})


class CategoryProfitApiView(View):
    def get(self, request, *args, **kwargs):
        window = request.GET.get("window", "15m")
        data = get_category_profits(window=window)
        return JsonResponse({"results": data})


class ChannelProfitApiView(View):
    def get(self, request, *args, **kwargs):
        window = request.GET.get("window", "15m")
        data = get_channel_profits(window=window)
        return JsonResponse({"results": data})


@method_decorator(csrf_exempt, name="dispatch")
class OrderIngestionApiView(View):
    def post(self, request, *args, **kwargs):
        try:
            body = request.body.decode("utf-8")
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            return JsonResponse(
                {"detail": "Invalid JSON payload."},
                status=400,
            )

        try:
            result = OrderIngestionService.process(payload)

            return JsonResponse(
                {
                    "success": True,
                    "created": result.created,
                    "duplicated": result.duplicated,
                    "idempotency_reason": result.idempotency_reason,
                    "order": {
                        "id": result.order.id if result.order else None,
                        "external_id": result.order.external_id if result.order else None,
                    },
                },
                status=200,
            )
        except KeyError as exc:
            return JsonResponse(
                {
                    "success": False,
                    "detail": f"Missing required field: {exc}",
                },
                status=400,
            )
        except Exception as exc:
            return JsonResponse(
                {
                    "success": False,
                    "detail": str(exc),
                },
                status=500,
            )
