from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


SCHEMA = json.loads(
    (Path(__file__).resolve().parents[1] / "contracts" / "order-event-v1.schema.json").read_text()
)
VALIDATOR = Draft202012Validator(SCHEMA)


@dataclass(frozen=True)
class MessageMeta:
    topic: str
    partition: int
    offset: int

    @property
    def idempotency_key(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass(frozen=True)
class ProcessingDecision:
    action: str
    idempotency_key: str
    event: dict[str, Any] | None = None
    error: str | None = None


def validate_event(payload: bytes | str) -> dict[str, Any]:
    try:
        event = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError(f"invalid_json: {exc}") from exc

    errors = sorted(VALIDATOR.iter_errors(event), key=lambda e: list(e.path))
    if errors:
        detail = "; ".join(error.message for error in errors)
        raise ValueError(f"schema_validation_failed: {detail}")
    return event


def decide(payload: bytes | str, meta: MessageMeta, processed: set[str]) -> ProcessingDecision:
    if meta.idempotency_key in processed:
        return ProcessingDecision("skip_duplicate", meta.idempotency_key)

    try:
        event = validate_event(payload)
    except ValueError as exc:
        return ProcessingDecision("dead_letter", meta.idempotency_key, error=str(exc))

    return ProcessingDecision("process_and_commit", meta.idempotency_key, event=event)


def dead_letter_envelope(payload: bytes | str, meta: MessageMeta, error: str) -> dict[str, Any]:
    raw = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    return {
        "source_topic": meta.topic,
        "source_partition": meta.partition,
        "source_offset": meta.offset,
        "idempotency_key": meta.idempotency_key,
        "error": error,
        "original_payload": raw,
    }
