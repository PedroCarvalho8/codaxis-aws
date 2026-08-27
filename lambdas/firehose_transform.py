import base64
import json
from datetime import datetime, timezone

REQUIRED = ("device_id", "event_time", "metric", "value")

def handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    out = []
    for rec in event["records"]:
        try:
            payload = json.loads(base64.b64decode(rec["data"]))
            if any(payload.get(f) is None for f in REQUIRED):
                out.append({"recordId": rec["recordId"],
                            "result": "ProcessingFailed"})
                continue
            payload["ingested_at"] = now
            payload["value"] = float(payload["value"])
            if payload.get("seq") is not None:
                payload["seq"] = int(payload["seq"])
            data = base64.b64encode(
                json.dumps(payload).encode("utf-8")
            ).decode("utf-8")
            out.append({"recordId": rec["recordId"],
                        "result": "Ok",
                        "data": data})
        except Exception:
            out.append({"recordId": rec["recordId"],
                        "result": "ProcessingFailed"})
    return {"records": out}
