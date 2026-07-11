import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from validation import ValidationError, summarize_payload, validate_payload


def _body(obj) -> bytes:
    return json.dumps(obj).encode("utf-8")


VALID_PAYLOAD = {
    "data": {
        "metrics": [
            {
                "name": "heart_rate",
                "units": "bpm",
                "data": [{"date": "2026-07-07 08:00:00 +0000", "avg": 72, "min": 68, "max": 85}],
            },
            {
                "name": "step_count",
                "units": "count",
                "data": [{"date": "2026-07-07 08:00:00 +0000", "qty": 1200}],
            },
        ],
        "workouts": [{"name": "Running", "start": "2026-07-07 07:00:00 +0000"}],
    }
}


def test_valid_payload_passes():
    payload = validate_payload(_body(VALID_PAYLOAD))
    assert payload == VALID_PAYLOAD


def test_empty_body_rejected():
    with pytest.raises(ValidationError, match="empty"):
        validate_payload(b"")


def test_invalid_json_rejected():
    with pytest.raises(ValidationError, match="not valid JSON"):
        validate_payload(b"{not json")


def test_non_object_payload_rejected():
    with pytest.raises(ValidationError, match="JSON object"):
        validate_payload(_body([1, 2, 3]))


def test_missing_data_key_rejected():
    with pytest.raises(ValidationError, match="'data'"):
        validate_payload(_body({"foo": "bar"}))


def test_data_not_object_rejected():
    with pytest.raises(ValidationError, match="'data'"):
        validate_payload(_body({"data": "oops"}))


def test_missing_metrics_key_rejected():
    with pytest.raises(ValidationError, match="metrics"):
        validate_payload(_body({"data": {}}))


def test_empty_metrics_list_rejected():
    with pytest.raises(ValidationError, match="non-empty"):
        validate_payload(_body({"data": {"metrics": []}}))


def test_metrics_not_a_list_rejected():
    with pytest.raises(ValidationError, match="metrics"):
        validate_payload(_body({"data": {"metrics": "heart_rate"}}))


def test_metric_missing_name_rejected():
    payload = {"data": {"metrics": [{"data": [{"date": "x", "avg": 1}]}]}}
    with pytest.raises(ValidationError, match="'name'"):
        validate_payload(_body(payload))


def test_metric_missing_data_array_rejected():
    payload = {"data": {"metrics": [{"name": "heart_rate"}]}}
    with pytest.raises(ValidationError, match="'data' array"):
        validate_payload(_body(payload))


def test_metric_not_object_rejected():
    payload = {"data": {"metrics": ["heart_rate"]}}
    with pytest.raises(ValidationError, match="must be an object"):
        validate_payload(_body(payload))


def test_workouts_optional():
    payload = {"data": {"metrics": VALID_PAYLOAD["data"]["metrics"]}}
    validate_payload(_body(payload))  # no workouts key - should not raise


def test_workouts_not_a_list_rejected():
    payload = {"data": {"metrics": VALID_PAYLOAD["data"]["metrics"], "workouts": {}}}
    with pytest.raises(ValidationError, match="workouts"):
        validate_payload(_body(payload))


def test_summarize_payload_counts():
    summary = summarize_payload(VALID_PAYLOAD)
    assert summary["metric_count"] == 2
    assert summary["workout_count"] == 1
    assert summary["metric_names"] == ["heart_rate", "step_count"]


def test_summarize_payload_no_workouts():
    payload = {"data": {"metrics": VALID_PAYLOAD["data"]["metrics"]}}
    summary = summarize_payload(payload)
    assert summary["workout_count"] == 0
