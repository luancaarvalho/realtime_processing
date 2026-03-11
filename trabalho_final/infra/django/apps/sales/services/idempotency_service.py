from __future__ import annotations

import hashlib, json
from dataclasses import dataclass
from typing import Any

from django.db import IntegrityError, transaction
from django.utils import timezone

from ..models import IdempotencyKey
from ..choices import IdempotencyStatus


@dataclass(frozen=True)
class IdempotencyAcquireResult:
    record: IdempotencyKey
    should_process: bool
    reason: str


class IdempotencyService:
    """
    Responsável por controlar a aquisição e o fechamento de uma chave
    de idempotência.

    Regras padrão:
    - Se a chave não existe: cria em PROCESSING e permite processar.
    - Se já existe em PROCESSED: não processa novamente.
    - Se já existe em PROCESSING: não processa novamente.
    - Se já existe em FAILED:
        - por padrão, permite retry e volta para PROCESSING.
    """

    @staticmethod
    def build_payload_hash(payload: dict[str, Any] | list[Any] | None) -> str:
        if payload is None:
            return ""

        normalized = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def build_key(
        *, source, resource_type, resource_id, event_id,
    ):
        """
        Gera uma chave estável para idempotência.

        Exemplos:
        - live:order:123
        - erp:order:123:event:550e8400-e29b-41d4-a716-446655440000
        """
        base = f"{source}:{resource_type}:{resource_id}"
        if event_id:
            return f"{base}:event:{event_id}"

        return base

    @classmethod
    def acquire(
        cls, *, key, source, event_id, resource_type, resource_id, payload_hash, allow_retry_failed,
    ):
        """
        Tenta adquirir a chave para processamento.
        """
        try:
            with transaction.atomic():
                record = IdempotencyKey.objects.create(
                    key=key,
                    source=source,
                    event_id=event_id,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    payload_hash=payload_hash,
                    status=IdempotencyStatus.PROCESSING,
                )
            return IdempotencyAcquireResult(
                record=record,
                should_process=True,
                reason="acquired",
            )
            
        except IntegrityError:
            record = IdempotencyKey.objects.get(key=key)

            if record.status == IdempotencyStatus.PROCESSED:
                return IdempotencyAcquireResult(
                    record=record,
                    should_process=False,
                    reason="already_processed",
                )

            if record.status == IdempotencyStatus.PROCESSING:
                return IdempotencyAcquireResult(
                    record=record,
                    should_process=False,
                    reason="already_processing",
                )

            if record.status == IdempotencyStatus.FAILED and allow_retry_failed:
                record.status = IdempotencyStatus.PROCESSING
                record.source = source or record.source
                record.event_id = event_id or record.event_id
                record.resource_type = resource_type or record.resource_type
                record.resource_id = resource_id or record.resource_id
                record.payload_hash = payload_hash or record.payload_hash
                record.processed_at = None
                record.response_code = None
                record.error_message = ""
                record.save(
                    update_fields=[
                        "status",
                        "source",
                        "event_id",
                        "resource_type",
                        "resource_id",
                        "payload_hash",
                        "processed_at",
                        "response_code",
                        "error_message",
                        "updated_at",
                    ]
                )
                return IdempotencyAcquireResult(
                    record=record,
                    should_process=True,
                    reason="retry_after_failure",
                )

            return IdempotencyAcquireResult(
                record=record,
                should_process=False,
                reason="not_allowed",
            )

    @staticmethod
    def mark_processed(record, response_code):
        record.status = IdempotencyStatus.PROCESSED
        record.response_code = response_code
        record.processed_at = timezone.now()
        record.error_message = ""
        record.save(
            update_fields=[
                "status",
                "response_code",
                "processed_at",
                "error_message",
                "updated_at",
            ]
        )

    @staticmethod
    def mark_failed(record, error_message, response_code):
        record.status = IdempotencyStatus.FAILED
        record.response_code = response_code
        record.processed_at = timezone.now()
        record.error_message = (error_message or "")[:5000]
        record.save(
            update_fields=[
                "status",
                "response_code",
                "processed_at",
                "error_message",
                "updated_at",
            ]
        )
