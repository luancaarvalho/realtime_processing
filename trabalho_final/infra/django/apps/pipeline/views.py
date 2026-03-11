from __future__ import annotations

from django.http import JsonResponse, HttpRequest
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from apps.pipeline.services.generator_client import GeneratorClient


def _normalize_status_payload(payload: dict | None) -> dict:
    payload = payload or {}
    status = payload.get("status", payload)

    running = bool(status.get("running", False))
    sent = int(status.get("sent", 0) or 0)
    errors = int(status.get("errors", 0) or 0)

    return {
        "running": running,
        "events_per_second": status.get("events_per_second", 0),
        "orders_processed": status.get("orders_processed", sent),
        "producer_status": status.get("producer_status", "Ativo" if running else "offline"),
        "consumer_status": status.get("consumer_status", "Ativo" if running else "offline"),
        "sent": sent,
        "errors": errors,
        "last_error": status.get("last_error"),
        "last_external_id": status.get("last_external_id"),
        "last_event_id": status.get("last_event_id"),
    }


@method_decorator(csrf_exempt, name="dispatch")
class PipelineStartView(View):
    def post(self, request: HttpRequest, *args, **kwargs) -> JsonResponse:
        client = GeneratorClient()

        try:
            payload = client.start() or {}
            normalized = _normalize_status_payload(payload)

            return JsonResponse(
                {
                    "success": True,
                    "message": payload.get("message", "Geração de dados iniciada com sucesso."),
                    "is_running": normalized["running"],
                    "generator_payload": normalized,
                },
                status=200,
            )
        except Exception as exc:
            return JsonResponse(
                {
                    "success": False,
                    "message": "Não foi possível iniciar a geração de dados.",
                    "detail": str(exc),
                    "is_running": False,
                    "generator_payload": _normalize_status_payload({}),
                },
                status=500,
            )


@method_decorator(csrf_exempt, name="dispatch")
class PipelineStopView(View):
    def post(self, request: HttpRequest, *args, **kwargs) -> JsonResponse:
        client = GeneratorClient()

        try:
            payload = client.stop() or {}
            normalized = _normalize_status_payload(payload)

            return JsonResponse(
                {
                    "success": True,
                    "message": payload.get("message", "Geração de dados parada com sucesso."),
                    "is_running": normalized["running"],
                    "generator_payload": normalized,
                },
                status=200,
            )
        except Exception as exc:
            return JsonResponse(
                {
                    "success": False,
                    "message": "Não foi possível parar a geração de dados.",
                    "detail": str(exc),
                    "is_running": False,
                    "generator_payload": _normalize_status_payload({}),
                },
                status=500,
            )


class PipelineStatusView(View):
    def get(self, request: HttpRequest, *args, **kwargs) -> JsonResponse:
        client = GeneratorClient()

        try:
            payload = client.status() or {}
            normalized = _normalize_status_payload(payload)

            return JsonResponse(
                {
                    "success": True,
                    "is_running": normalized["running"],
                    "generator_payload": normalized,
                },
                status=200,
            )
        except Exception as exc:
            return JsonResponse(
                {
                    "success": False,
                    "detail": str(exc),
                    "is_running": False,
                    "generator_payload": _normalize_status_payload({}),
                },
                status=200,
            )
