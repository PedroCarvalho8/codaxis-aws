import json
from datetime import datetime, timezone
from http.client import responses
import os

import boto3
from boto3.dynamodb.conditions import Key

TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])


def resp(status, corpo):
    return {"statusCode": status,
            "headers": {"content-type": "application/json",
                        "cache-control": "no-store"},
            "body": json.dumps(corpo)}


def erro(status, mensagem, caminho, campos=None):
    """Erro no formato ApiError da spec (openapi/openapi.yaml)."""
    return resp(status, {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status, "error": responses.get(status, "Error"),
        "message": mensagem, "path": caminho, "fields": campos or {}})


def molda(item):
    """Item DEVICEMETA -> DeviceResponse do contrato argus."""
    return {
        "id": item["sk"],
        "code": item["sk"],
        "label": item.get("label") or "",
        "createdAt": item.get("created_at"),
        "revokedAt": item.get("revoked_at"),
        "active": bool(item.get("active")),
    }


def handler(event, context):
    rota = event.get("routeKey", "")
    caminho_url = event.get("rawPath", rota)
    caminho = event.get("pathParameters") or {}

    if rota == "GET /v1/devices":
        # Metadados numa particao unica: listar e uma Query, nao um Scan nem
        # N chamadas ao IoT. A particao cresce com a frota, nao com o dado.
        itens, proxima = [], None
        while True:
            extra = {"ExclusiveStartKey": proxima} if proxima else {}
            pagina = TABELA.query(
                KeyConditionExpression=Key("pk").eq("DEVICEMETA"), **extra
            )
            itens += pagina["Items"]
            proxima = pagina.get("LastEvaluatedKey")
            if not proxima:
                break
        return resp(200, [molda(i) for i in itens])

    if rota == "GET /v1/devices/{code}":
        codigo = caminho.get("code")
        r = TABELA.get_item(Key={"pk": "DEVICEMETA", "sk": codigo})
        if "Item" not in r:
            return erro(404, f"device {codigo} nao existe", caminho_url)
        return resp(200, molda(r["Item"]))

    return erro(404, "rota desconhecida", caminho_url)
