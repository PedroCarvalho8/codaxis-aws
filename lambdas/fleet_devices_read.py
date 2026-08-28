import json

import boto3

iot = boto3.client("iot")
# Autenticacao no JWT authorizer nativo do gateway (Cognito User Pool):
# requisicao sem token valido nem chega a invocar esta funcao.
_cache = {}


def resposta(status, corpo):
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json",
                    "cache-control": "no-store"},
        "body": json.dumps(corpo),
    }


def endpoint_mqtt():
    if "ep" not in _cache:
        _cache["ep"] = iot.describe_endpoint(
            endpointType="iot:Data-ATS"
        )["endpointAddress"]
    return _cache["ep"]


def certificados_de(nome):
    principais = iot.list_thing_principals(thingName=nome)["principals"]
    saida = []
    for arn in principais:
        cert_id = arn.split("/")[-1]
        desc = iot.describe_certificate(
            certificateId=cert_id
        )["certificateDescription"]
        saida.append({
            "certificate_id": cert_id,
            "status": desc["status"],
            "created_at": desc["creationDate"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return saida


def handler(event, context):
    rota = event.get("routeKey", "")
    caminho = event.get("pathParameters") or {}
    consulta = event.get("queryStringParameters") or {}

    if rota == "GET /fleet/devices":
        extra = {}
        if consulta.get("next"):
            extra["nextToken"] = consulta["next"]
        pagina = iot.list_things(maxResults=100, **extra)
        return resposta(200, {
            "devices": [t["thingName"] for t in pagina.get("things", [])],
            "next": pagina.get("nextToken"),
        })

    if rota == "GET /fleet/devices/{device_id}":
        nome = caminho.get("device_id")
        try:
            iot.describe_thing(thingName=nome)
        except iot.exceptions.ResourceNotFoundException:
            return resposta(404, {"erro": f"device {nome} nao existe"})
        return resposta(200, {
            "device_id": nome,
            "endpoint": endpoint_mqtt(),
            "topics": {
                "telemetry": f"devices/{nome}/telemetry",
                "position": f"devices/{nome}/position",
            },
            "certificates": certificados_de(nome),
        })

    return resposta(404, {"erro": "rota desconhecida"})
