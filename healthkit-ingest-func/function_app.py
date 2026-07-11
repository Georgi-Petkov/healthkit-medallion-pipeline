import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import azure.functions as func
from azure.core.exceptions import AzureError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from validation import ValidationError, summarize_payload, validate_payload

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

logger = logging.getLogger("healthkit-ingest")

ADLS_ACCOUNT_URL = os.environ.get("ADLS_ACCOUNT_URL")
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "bronze")
API_KEY_HEADER = "X-Api-Key"
EXPECTED_API_KEY = os.environ.get("HEALTHKIT_API_KEY")

# Module-level so the credential/client are created once per worker process
# and reused across invocations, per Azure Functions Python best practices.
_credential = DefaultAzureCredential()
_service_client: DataLakeServiceClient | None = None


def _get_service_client() -> DataLakeServiceClient:
    global _service_client
    if _service_client is None:
        if not ADLS_ACCOUNT_URL:
            raise RuntimeError("ADLS_ACCOUNT_URL app setting is not configured")
        _service_client = DataLakeServiceClient(
            account_url=ADLS_ACCOUNT_URL,
            credential=_credential,
            # Explicit retry/backoff for transient errors on the storage call
            # itself (separate from any HTTP-layer retry the caller does).
            retry_total=4,
            retry_backoff_factor=0.8,
            retry_backoff_max=8,
        )
    return _service_client


def _blob_path(now: datetime, request_id: str) -> str:
    return (
        f"raw/healthkit/{now:%Y}/{now:%m}/{now:%d}/"
        f"{now:%Y%m%d_%H%M%S}_{request_id}.json"
    )


def _log(level: int, message: str, **fields) -> None:
    logger.log(level, json.dumps({"message": message, **fields}))


def _json_response(body: dict, status_code: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body), status_code=status_code, mimetype="application/json"
    )


@app.function_name(name="HealthKitIngest")
@app.route(route="ingest", methods=["POST"])
def ingest(req: func.HttpRequest) -> func.HttpResponse:
    request_id = str(uuid.uuid4())
    started = time.monotonic()

    if not EXPECTED_API_KEY:
        _log(logging.ERROR, "HEALTHKIT_API_KEY is not configured", request_id=request_id)
        return _json_response({"error": "server misconfigured"}, 500)

    supplied_key = req.headers.get(API_KEY_HEADER, "")
    if not hmac.compare_digest(supplied_key, EXPECTED_API_KEY):
        _log(logging.WARNING, "Unauthorized ingest request", request_id=request_id)
        return _json_response({"error": "unauthorized"}, 401)

    raw_body = req.get_body()
    payload_size = len(raw_body)

    try:
        payload = validate_payload(raw_body)
    except ValidationError as exc:
        _log(
            logging.WARNING,
            "Payload validation failed",
            request_id=request_id,
            payload_size=payload_size,
            error=str(exc),
        )
        return _json_response({"error": "invalid payload", "detail": str(exc)}, 400)

    summary = summarize_payload(payload)
    content_hash = hashlib.sha256(raw_body).hexdigest()
    now = datetime.now(timezone.utc)
    blob_path = _blob_path(now, request_id)

    write_started = time.monotonic()
    try:
        file_system_client = _get_service_client().get_file_system_client(BRONZE_CONTAINER)
        file_client = file_system_client.get_file_client(blob_path)
        # Upload the raw bytes as received - no re-serialization/transform,
        # so the Bronze copy is byte-for-byte what the app sent.
        file_client.upload_data(
            raw_body,
            overwrite=True,
            metadata={
                "request_id": request_id,
                "content_sha256": content_hash,
                "ingested_at": now.isoformat(),
                "metric_count": str(summary["metric_count"]),
                "workout_count": str(summary["workout_count"]),
                "source": "health-auto-export",
            },
        )
    except AzureError as exc:
        _log(
            logging.ERROR,
            "Storage write failed",
            request_id=request_id,
            blob_path=blob_path,
            error=str(exc),
        )
        return _json_response({"error": "storage write failed"}, 500)

    write_duration_ms = round((time.monotonic() - write_started) * 1000, 1)
    total_duration_ms = round((time.monotonic() - started) * 1000, 1)

    _log(
        logging.INFO,
        "Ingest succeeded",
        request_id=request_id,
        payload_size=payload_size,
        metric_count=summary["metric_count"],
        workout_count=summary["workout_count"],
        content_sha256=content_hash,
        blob_path=blob_path,
        write_duration_ms=write_duration_ms,
        total_duration_ms=total_duration_ms,
    )

    return _json_response(
        {"status": "ok", "request_id": request_id, "path": blob_path}, 200
    )
