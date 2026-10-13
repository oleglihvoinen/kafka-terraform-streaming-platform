import json
import pytest

from src.streaming import MessageMeta, dead_letter_envelope, decide, validate_event


VALID = {
    "event_id": "e1",
    "event_version": 1,
    "event_type": "order_created",
    "occurred_at": "2026-10-13T10:00:00Z",
    "order_id": "O100",
    "customer_id": "C10",
    "amount": 129.90,
    "currency": "EUR"
}


def test_valid_event_passes_contract():
    assert validate_event(json.dumps(VALID))["order_id"] == "O100"


def test_contract_rejects_unknown_fields():
    event = dict(VALID, secret="not-allowed")
    with pytest.raises(ValueError):
        validate_event(json.dumps(event))


def test_duplicate_transport_position_is_not_reprocessed():
    meta = MessageMeta("orders.v1", 2, 44)
    decision = decide(json.dumps(VALID), meta, {meta.idempotency_key})
    assert decision.action == "skip_duplicate"


def test_invalid_message_is_dead_lettered_without_commit_decision():
    meta = MessageMeta("orders.v1", 0, 7)
    decision = decide("not-json", meta, set())
    assert decision.action == "dead_letter"
    dlq = dead_letter_envelope("not-json", meta, decision.error or "")
    assert dlq["source_offset"] == 7
    assert "invalid_json" in dlq["error"]


def test_valid_message_is_processed_before_commit():
    decision = decide(json.dumps(VALID), MessageMeta("orders.v1", 0, 1), set())
    assert decision.action == "process_and_commit"
