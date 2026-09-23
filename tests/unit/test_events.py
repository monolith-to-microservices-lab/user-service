"""Unit tests for the Debezium event parser (app/cdc/events.py) in isolation.

Scope boundary (deliberately not mixed with apply.py's responsibility):
- events.py: STRUCTURAL parsing only - is this valid Pydantic data? Malformed
  JSON, missing required payload fields, or wrong types must raise here.
- apply.py: SEMANTIC validation - is `after` present for a create, is `op`
  one this system knows how to apply? That is covered in test_apply.py.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from app.cdc.events import DebeziumUserEnvelope, UserCdcPayload

VALID_AFTER = {"id": 123, "name": "Alice", "created_at": "2026-01-01T00:00:00Z"}


class TestSuccessCases:
    def test_create_event(self):
        envelope = DebeziumUserEnvelope.model_validate(
            {"before": None, "after": VALID_AFTER, "op": "c"}
        )
        assert envelope.op == "c"
        assert envelope.after is not None
        assert envelope.after.id == 123
        assert envelope.before is None

    def test_read_snapshot_event(self):
        """op=r (snapshot read) parses the same shape as create - the
        connector runs with snapshot.mode=no_data so this is never produced
        by this pipeline in practice, but the model must not special-case it
        away; apply.py treats it as an upsert (see _UPSERT_OPS).
        """
        envelope = DebeziumUserEnvelope.model_validate(
            {"before": None, "after": VALID_AFTER, "op": "r"}
        )
        assert envelope.op == "r"
        assert envelope.after.id == 123

    def test_update_event_has_both_before_and_after(self):
        before = {**VALID_AFTER, "name": "Old Name"}
        envelope = DebeziumUserEnvelope.model_validate(
            {"before": before, "after": VALID_AFTER, "op": "u"}
        )
        assert envelope.op == "u"
        assert envelope.before.name == "Old Name"
        assert envelope.after.name == "Alice"

    def test_delete_event_uses_before_after_is_none(self):
        envelope = DebeziumUserEnvelope.model_validate(
            {"before": VALID_AFTER, "after": None, "op": "d"}
        )
        assert envelope.op == "d"
        assert envelope.before.id == 123
        assert envelope.after is None

    def test_source_ts_ms_is_captured_for_latency(self):
        envelope = DebeziumUserEnvelope.model_validate(
            {
                "after": VALID_AFTER,
                "op": "c",
                "source": {"ts_ms": 1700000000123},
                "ts_ms": 1700000000456,
            }
        )
        assert envelope.source.ts_ms == 1700000000123
        assert envelope.ts_ms == 1700000000456

    def test_unknown_extra_fields_are_ignored(self):
        """The real Debezium envelope carries many more fields (transaction,
        full source block with lsn/txId/etc) - the model only needs the ones
        it uses, and must not break when the connector adds more.
        """
        envelope = DebeziumUserEnvelope.model_validate(
            {
                "before": None,
                "after": VALID_AFTER,
                "op": "c",
                "transaction": None,
                "source": {"ts_ms": 1, "lsn": 123456, "txId": 42, "table": "users"},
            }
        )
        assert envelope.op == "c"


class TestFailureCases:
    """Malformed events must fail loudly (raise), never be silently ignored -
    the consumer's caller decides what to do with the exception (currently:
    let it propagate and crash the process, same as any other apply
    failure - see consumer.py's process_message).
    """

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            json.loads(b"{not valid json")

    def test_missing_op_raises(self):
        with pytest.raises(ValidationError):
            DebeziumUserEnvelope.model_validate({"before": None, "after": VALID_AFTER})

    def test_op_is_required_to_be_a_string(self):
        with pytest.raises(ValidationError):
            DebeziumUserEnvelope.model_validate({"after": VALID_AFTER, "op": 123})

    def test_unknown_op_value_parses_but_is_rejected_later_by_apply(self):
        """events.py does not restrict `op` to a known set (Debezium could in
        principle add a new one) - it parses fine here. apply.py is where an
        unrecognized op is rejected (see test_apply.py::test_unknown_op_raises).
        """
        envelope = DebeziumUserEnvelope.model_validate({"after": VALID_AFTER, "op": "x"})
        assert envelope.op == "x"

    def test_payload_missing_id_raises(self):
        with pytest.raises(ValidationError):
            DebeziumUserEnvelope.model_validate(
                {"after": {"name": "Alice", "created_at": "2026-01-01T00:00:00Z"}, "op": "c"}
            )

    def test_payload_missing_name_raises(self):
        with pytest.raises(ValidationError):
            DebeziumUserEnvelope.model_validate(
                {"after": {"id": 1, "created_at": "2026-01-01T00:00:00Z"}, "op": "c"}
            )

    def test_payload_id_wrong_type_raises(self):
        with pytest.raises(ValidationError):
            UserCdcPayload.model_validate(
                {"id": "not-a-number", "name": "Alice", "created_at": "2026-01-01T00:00:00Z"}
            )

    def test_payload_invalid_timestamp_raises(self):
        with pytest.raises(ValidationError):
            UserCdcPayload.model_validate(
                {"id": 1, "name": "Alice", "created_at": "not-a-timestamp"}
            )

    def test_empty_dict_raises(self):
        with pytest.raises(ValidationError):
            DebeziumUserEnvelope.model_validate({})
