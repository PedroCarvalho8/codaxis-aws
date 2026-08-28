import base64
import json
import os
from datetime import datetime, timedelta, timezone

import boto3

TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])
TTL_DIAS = int(os.environ["TTL_DAYS"])

# lat/lon sao obrigatorios e precisam vir PAREADOS na mesma leitura: distancia,
# velocidade e a propria celula do mapa dependem do par, e nenhum deles se
# reconstroi se os dois virarem registros separados.
REQUIRED = ("device_id", "event_time", "lat", "lon")
OPCIONAIS_FLOAT = ("speed", "heading", "altitude")


def grava_dynamo(validos):
    """Rastro (TRACK) e ultima posicao (LATEST) em tempo quase real.

    O Iceberg continua sendo a fonte historica completa; estas linhas servem
    a API do argus (latest e paginacao do track) sem esperar o job horario.
    Escrita idempotente por (pk, sk): retry do Firehose sobrescreve igual.
    """
    from decimal import Decimal
    expira = int((datetime.now(timezone.utc)
                  + timedelta(days=TTL_DIAS)).timestamp())
    ultimos = {}
    with TABELA.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as lote:
        for p in validos:
            item = {"ts": p["event_time"],
                    "lat": Decimal(str(p["lat"])),
                    "lon": Decimal(str(p["lon"])),
                    "received_at": p["ingested_at"][:19] + "Z",
                    "expires_at": expira}
            for campo in ("speed", "heading"):
                if p.get(campo) is not None:
                    item[campo] = Decimal(str(p[campo]))
            lote.put_item(Item=dict(
                item, pk=f"TRACK#{p['device_id']}",
                sk=f"{p['event_time']}#{int(p.get('seq') or 0):06d}"))
            atual = ultimos.get(p["device_id"])
            if not atual or p["event_time"] > atual["ts"]:
                ultimos[p["device_id"]] = item
        for device, item in ultimos.items():
            lote.put_item(Item=dict(
                item, pk=f"DEV#{device}#position", sk="LATEST",
                at=item["ts"]))


def handler(event, context):
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    out = []
    validos = []
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
            validos.append(payload)
        except Exception:
            out.append({"recordId": rec["recordId"],
                        "result": "ProcessingFailed"})
    if validos:
        grava_dynamo(validos)
    return {"records": out}
