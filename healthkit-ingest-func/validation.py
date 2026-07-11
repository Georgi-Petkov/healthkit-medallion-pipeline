"""Parsing and validation for Health Auto Export (v2) payloads.

Kept dependency-free (stdlib json only) so it can be unit tested without
touching the Azure SDKs or a running Function host.
"""
import json
from typing import Any


class ValidationError(Exception):
    """Raised when a payload fails structural validation."""


def validate_payload(raw_body: bytes) -> dict:
    """Parse raw request bytes and validate Health Auto Export v2 shape.

    Returns the parsed payload dict on success. Raises ValidationError,
    with a message safe to return to the caller, on any failure.
    """
    if not raw_body:
        raise ValidationError("Request body is empty")

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValidationError(f"Body is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValidationError("Payload must be a JSON object")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValidationError("Payload is missing a 'data' object")

    metrics = data.get("metrics")
    if not isinstance(metrics, list) or len(metrics) == 0:
        raise ValidationError("'data.metrics' must be a non-empty array")

    for i, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            raise ValidationError(f"metrics[{i}] must be an object")
        if not metric.get("name"):
            raise ValidationError(f"metrics[{i}] is missing a 'name' field")
        if not isinstance(metric.get("data"), list):
            raise ValidationError(f"metrics[{i}] is missing a 'data' array")

    workouts = data.get("workouts")
    if workouts is not None and not isinstance(workouts, list):
        raise ValidationError("'data.workouts' must be an array if present")

    return payload


def summarize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract counters used for logging/observability from a validated payload."""
    data = payload["data"]
    metrics = data.get("metrics", [])
    workouts = data.get("workouts") or []
    metric_names = sorted({m["name"] for m in metrics if isinstance(m, dict) and m.get("name")})
    return {
        "metric_count": len(metrics),
        "workout_count": len(workouts),
        "metric_names": metric_names,
    }
