import json
import os
import re
from datetime import datetime, timezone

import boto3

iot = boto3.client("iot")
TABELA = boto3.resource("dynamodb").Table(os.environ["TABLE"])
POLICY = os.environ["DEVICE_POLICY"]
CODIGO_OK = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")


def resp(status, corpo):
    return {"statusCode": status,
            "headers": {"content-type": "application/json",
                        "cache-control": "no-store"},
            "body": json.dumps(corpo)}


def agora():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def molda(item):
    return {"id": item["sk"], "code": item["sk"],
            "label": item.get("label") or "",
            "createdAt": item.get("created_at"),
            "revokedAt": item.get("revoked_at"),
            "active": bool(item.get("active"))}


def handler(event, context):
    rota = event.get("routeKey", "")
    caminho = event.get("pathParameters") or {}
    codigo = caminho.get("code")
    try:
        corpo = json.loads(event.get("body") or "{}")
    except ValueError:
        return resp(400, {"message": "body invalido"})

    if rota == "POST /api/devices":
        codigo = corpo.get("code") or ""
        rotulo = (corpo.get("label") or "").strip()
        if not CODIGO_OK.match(codigo):
            return resp(400, {"message": "code: [a-zA-Z0-9_-]{1,128}"})
        if not rotulo:
            return resp(400, {"message": "label obrigatorio"})
        try:
            iot.create_thing(thingName=codigo)
        except iot.exceptions.ResourceAlreadyExistsException:
            return resp(409, {"message": f"device {codigo} ja existe"})
        cert = iot.create_keys_and_certificate(setAsActive=True)
        iot.attach_policy(policyName=POLICY, target=cert["certificateArn"])
        iot.attach_thing_principal(
            thingName=codigo, principal=cert["certificateArn"]
        )
        # Label vive aqui, nao em atributo do thing: atributo de thing nao
        # aceita espaco, e rotulo humano tem espaco.
        item = {"pk": "DEVICEMETA", "sk": codigo, "label": rotulo,
                "created_at": agora(), "revoked_at": None, "active": True}
        TABELA.put_item(Item=item)
        ep = iot.describe_endpoint(
            endpointType="iot:Data-ATS"
        )["endpointAddress"]
        # O contrato tem UM campo de segredo. O bundle vai inteiro nele:
        # chave privada + certificado + endpoint, mostrado uma unica vez.
        chave = (f"# endpoint: {ep}\n# client id: {codigo}\n"
                 f"{cert['keyPair']['PrivateKey']}\n{cert['certificatePem']}")
        return resp(201, {"device": molda(item), "key": chave})

    if rota == "DELETE /api/devices/{code}/key":
        r = TABELA.get_item(Key={"pk": "DEVICEMETA", "sk": codigo})
        if "Item" not in r:
            return resp(404, {"message": f"device {codigo} nao existe"})
        try:
            arns = iot.list_thing_principals(thingName=codigo)["principals"]
        except iot.exceptions.ResourceNotFoundException:
            arns = []
        for arn in arns:
            cid = arn.split("/")[-1]
            # Desativa antes de tudo: derruba a conexao mesmo se um passo
            # seguinte falhar.
            iot.update_certificate(certificateId=cid, newStatus="INACTIVE")
            iot.detach_policy(policyName=POLICY, target=arn)
            iot.detach_thing_principal(thingName=codigo, principal=arn)
            iot.delete_certificate(certificateId=cid)
        item = dict(r["Item"], revoked_at=agora(), active=False)
        TABELA.put_item(Item=item)
        return resp(200, molda(item))

    return resp(404, {"message": "rota desconhecida"})
