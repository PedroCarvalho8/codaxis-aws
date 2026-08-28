import json
import os
import re

import boto3

iot = boto3.client("iot")
POLICY = os.environ["DEVICE_POLICY"]
NOME_OK = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")
# Autenticacao no JWT authorizer nativo do gateway (Cognito User Pool):
# requisicao sem token valido nem chega a invocar esta funcao.


def resp(status, corpo):
    return {"statusCode": status,
            "headers": {"content-type": "application/json",
                        "cache-control": "no-store"},
            "body": json.dumps(corpo)}


def emite(nome):
    # Cria par de chaves ja ativo, anexa a policy da frota e o thing.
    c = iot.create_keys_and_certificate(setAsActive=True)
    iot.attach_policy(policyName=POLICY, target=c["certificateArn"])
    iot.attach_thing_principal(thingName=nome, principal=c["certificateArn"])
    ep = iot.describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    return {"device_id": nome,
            "certificate_id": c["certificateId"],
            # Chave privada devolvida SO aqui; nao e armazenada.
            "certificate_pem": c["certificatePem"],
            "private_key": c["keyPair"]["PrivateKey"],
            "endpoint": ep,
            "topics": {"telemetry": f"devices/{nome}/telemetry",
                       "position": f"devices/{nome}/position"}}


def cert_de(nome, cert_id):
    for arn in iot.list_thing_principals(thingName=nome)["principals"]:
        if arn.split("/")[-1] == cert_id:
            return arn
    return None


def revoga(nome, arn):
    # Desativa antes de tudo: derruba a conexao mesmo se um passo falhar.
    cid = arn.split("/")[-1]
    iot.update_certificate(certificateId=cid, newStatus="INACTIVE")
    iot.detach_policy(policyName=POLICY, target=arn)
    iot.detach_thing_principal(thingName=nome, principal=arn)
    iot.delete_certificate(certificateId=cid)


def handler(event, context):
    rota = event.get("routeKey", "")
    pp = event.get("pathParameters") or {}
    nome = pp.get("device_id")
    try:
        corpo = json.loads(event.get("body") or "{}")
    except ValueError:
        return resp(400, {"erro": "body invalido"})

    try:
        if rota == "POST /fleet/devices":
            nome = corpo.get("device_id") or ""
            if not NOME_OK.match(nome):
                return resp(400, {"erro": "device_id: [a-zA-Z0-9_-]{1,128}"})
            try:
                iot.create_thing(thingName=nome)
            except iot.exceptions.ResourceAlreadyExistsException:
                return resp(409, {"erro": f"device {nome} ja existe"})
            return resp(201, emite(nome))

        if rota == "POST /fleet/devices/{device_id}/certificates":
            iot.describe_thing(thingName=nome)   # 404 se nao existir
            return resp(201, emite(nome))

        if rota.startswith("PATCH "):
            estado = corpo.get("status")
            if estado not in ("ACTIVE", "INACTIVE"):
                return resp(400, {"erro": "status: ACTIVE ou INACTIVE"})
            cid = pp.get("certificate_id")
            if not cert_de(nome, cid):
                return resp(404, {"erro": "certificado nao e deste device"})
            iot.update_certificate(certificateId=cid, newStatus=estado)
            return resp(200, {"certificate_id": cid, "status": estado})

        if rota.startswith("DELETE ") and "{certificate_id}" in rota:
            arn = cert_de(nome, pp.get("certificate_id"))
            if not arn:
                return resp(404, {"erro": "certificado nao e deste device"})
            revoga(nome, arn)
            return resp(200, {"revoked": pp.get("certificate_id")})

        if rota == "DELETE /fleet/devices/{device_id}":
            arns = iot.list_thing_principals(thingName=nome)["principals"]
            for arn in arns:
                revoga(nome, arn)
            iot.delete_thing(thingName=nome)
            return resp(200, {"deleted": nome, "certificates": len(arns)})
    except iot.exceptions.ResourceNotFoundException:
        return resp(404, {"erro": f"device {nome} nao existe"})

    return resp(404, {"erro": "rota desconhecida"})
