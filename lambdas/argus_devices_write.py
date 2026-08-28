import json
import os
import re
from datetime import datetime, timezone
from http.client import responses

import boto3

iot = boto3.client("iot")
TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])
POLICY = os.environ["DEVICE_POLICY"]
COD_OK = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")


def resp(status, corpo):
    return {"statusCode": status,
            "headers": {"content-type": "application/json",
                        "cache-control": "no-store"},
            "body": json.dumps(corpo)}


def erro(status, mensagem, caminho, campos=None):
    return resp(status, {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": status, "error": responses.get(status, "Error"),
        "message": mensagem, "path": caminho, "fields": campos or {}})


def agora():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def molda(i):
    return {"id": i["sk"], "code": i["sk"], "label": i.get("label") or "",
            "createdAt": i.get("created_at"),
            "revokedAt": i.get("revoked_at"), "active": bool(i.get("active"))}


def handler(event, context):
    rota = event.get("routeKey", "")
    caminho_url = event.get("rawPath", rota)
    caminho = event.get("pathParameters") or {}
    codigo = caminho.get("code")
    try:
        corpo = json.loads(event.get("body") or "{}")
    except ValueError:
        return erro(400, "body invalido", event.get("rawPath", ""))

    if rota == "POST /v1/devices":
        codigo = corpo.get("code") or ""
        rotulo = (corpo.get("label") or "").strip()
        if not COD_OK.match(codigo):
            return erro(400, "code invalido", caminho_url,
                        {"code": "[a-zA-Z0-9_-]{1,128}"})
        if not rotulo:
            return erro(400, "label obrigatorio", caminho_url,
                        {"label": "obrigatorio"})
        try:
            iot.create_thing(thingName=codigo)
        except iot.exceptions.ResourceAlreadyExistsException:
            return erro(409, f"device {codigo} ja existe", caminho_url)
        cert = iot.create_keys_and_certificate(setAsActive=True)
        iot.attach_policy(policyName=POLICY, target=cert["certificateArn"])
        iot.attach_thing_principal(
            thingName=codigo, principal=cert["certificateArn"]
        )
        # Label no DynamoDB: atributo de thing nao aceita espaco.
        item = {"pk": "DEVICEMETA", "sk": codigo, "label": rotulo,
                "created_at": agora(), "revoked_at": None, "active": True}
        TABELA.put_item(Item=item)
        ep = iot.describe_endpoint(
            endpointType="iot:Data-ATS"
        )["endpointAddress"]
        # Mostradas UMA vez: a chave privada nao fica armazenada.
        return resp(201, {"device": molda(item), "credentials": {
            "privateKeyPem": cert["keyPair"]["PrivateKey"],
            "certificatePem": cert["certificatePem"],
            "mqttEndpoint": ep,
            "clientId": codigo,
            "topics": {"telemetry": f"devices/{codigo}/telemetry",
                       "position": f"devices/{codigo}/position"}}})

    if rota == "DELETE /v1/devices/{code}/credentials":
        r = TABELA.get_item(Key={"pk": "DEVICEMETA", "sk": codigo})
        if "Item" not in r:
            return erro(404, f"device {codigo} nao existe", caminho_url)
        try:
            arns = iot.list_thing_principals(thingName=codigo)["principals"]
        except iot.exceptions.ResourceNotFoundException:
            arns = []
        for arn in arns:
            cid = arn.split("/")[-1]
            # Desativa primeiro: derruba a conexao mesmo se algo falhar.
            iot.update_certificate(certificateId=cid, newStatus="INACTIVE")
            iot.detach_policy(policyName=POLICY, target=arn)
            iot.detach_thing_principal(thingName=codigo, principal=arn)
            iot.delete_certificate(certificateId=cid)
        item = dict(r["Item"], revoked_at=agora(), active=False)
        TABELA.put_item(Item=item)
        return resp(200, molda(item))

    return erro(404, "rota desconhecida", caminho_url)
