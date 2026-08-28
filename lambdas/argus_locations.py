import json
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])
LIMITE_MAX = 500


def resp(status, corpo):
    return {"statusCode": status,
            "headers": {"content-type": "application/json",
                        "cache-control": "no-store"},
            "body": json.dumps(corpo)}


def molda(codigo, item):
    """Item TRACK/LATEST -> LocationResponse do contrato argus."""
    ts = item.get("ts") or item.get("at")
    return {
        # id numerico que o contrato pede; epoch ms do fix e unico o
        # bastante por device e estavel entre paginas.
        "id": int(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
                  .replace(tzinfo=timezone.utc).timestamp() * 1000),
        "deviceCode": codigo,
        "ts": ts,
        "lat": float(item["lat"]),
        "lon": float(item["lon"]),
        # Ingestao registra km/h (contrato MQTT); o argus espera m/s.
        "speedMps": (round(float(item["speed"]) / 3.6, 3)
                     if item.get("speed") is not None else None),
        "courseDeg": (float(item["heading"])
                      if item.get("heading") is not None else None),
        "sats": None,
        "hdop": None,
        "receivedAt": item.get("received_at") or ts,
    }


def handler(event, context):
    rota = event.get("routeKey", "")
    q = event.get("queryStringParameters") or {}

    if rota == "GET /api/locations/latest":
        # DEVICEMETA lista a frota; um GetItem por device pega o LATEST que a
        # ingestao mantem. Custo acompanha o tamanho da frota.
        frota = TABELA.query(
            KeyConditionExpression=Key("pk").eq("DEVICEMETA")
        )["Items"]
        saida = []
        for meta in frota:
            r = TABELA.get_item(
                Key={"pk": f"DEV#{meta['sk']}#position", "sk": "LATEST"}
            )
            if "Item" in r:
                saida.append(molda(meta["sk"], r["Item"]))
        return resp(200, saida)

    if rota == "GET /api/locations":
        codigo = q.get("device")
        if not codigo:
            return resp(400, {"message": "device obrigatorio"})
        limite = min(int(q.get("limit") or 100), LIMITE_MAX)
        cond = Key("pk").eq(f"TRACK#{codigo}")
        de, ate = q.get("from"), q.get("to")
        # `to` e EXCLUSIVO: e o cursor da paginacao keyset, entao a linha do
        # cursor nao pode reaparecer na pagina seguinte. between e inclusivo,
        # dai o filtro apos a Query quando from e to vem juntos.
        if de and ate:
            cond &= Key("sk").between(de, ate)
        elif ate:
            cond &= Key("sk").lt(ate)
        elif de:
            cond &= Key("sk").gte(de)
        pagina = TABELA.query(
            KeyConditionExpression=cond,
            ScanIndexForward=False,       # mais recente primeiro
            Limit=limite,
        )
        linhas = [i for i in pagina["Items"] if not ate or i["sk"] < ate]
        itens = [molda(codigo, i) for i in linhas]
        # Keyset: o proximo pedido repassa o cursor no parametro `to`.
        cursor = (linhas[-1]["sk"]
                  if linhas and pagina.get("LastEvaluatedKey") else None)
        return resp(200, {"items": itens, "nextCursor": cursor})

    return resp(404, {"message": "rota desconhecida"})
