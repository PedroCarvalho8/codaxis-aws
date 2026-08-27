import base64
import json
from datetime import datetime, timezone

# lat/lon sao obrigatorios e precisam vir PAREADOS na mesma leitura: distancia,
# velocidade e a propria celula do mapa dependem do par, e nenhum deles se
# reconstroi se os dois virarem registros separados.
REQUIRED = ("device_id", "event_time", "lat", "lon")
OPCIONAIS_FLOAT = ("speed", "heading", "altitude")


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
            lat = float(payload["lat"])
            lon = float(payload["lon"])
            if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
                # Coordenada fora do globo costuma ser fix de GPS invalido
                # (0,0 e o classico). Deixar entrar sujaria o heatmap com uma
                # celula no golfo da Guine.
                out.append({"recordId": rec["recordId"],
                            "result": "ProcessingFailed"})
                continue
            payload["lat"] = lat
            payload["lon"] = lon
            payload["ingested_at"] = now
            for campo in OPCIONAIS_FLOAT:
                if payload.get(campo) is not None:
                    payload[campo] = float(payload[campo])
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
