import json
from datetime import datetime, timezone
from http.client import responses
import os

import boto3

idp = boto3.client("cognito-idp")
POOL = os.environ["USER_POOL"]
PAPEIS = ("ADMIN", "OPERATOR", "VIEWER")


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


def papel_de(claims):
    return (claims or {}).get("role") or "VIEWER"


def atributos(usuario):
    return {a["Name"]: a["Value"] for a in usuario.get("Attributes")
            or usuario.get("UserAttributes") or []}


def molda(usuario, papel):
    at = atributos(usuario)
    return {
        "id": at.get("sub") or usuario["Username"],
        "email": at.get("email") or usuario["Username"],
        "name": at.get("name") or "",
        "role": papel,
        "createdAt": usuario["UserCreateDate"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "disabledAt": None if usuario.get("Enabled", True) else "",
    }


def handler(event, context):
    claims = (event.get("requestContext", {}).get("authorizer", {})
              .get("jwt", {}).get("claims", {}))
    # So ADMIN gere usuarios -- mesmo criterio que o webapp usa para exibir a
    # tela, mas imposto aqui, onde vale.
    if papel_de(claims) != "ADMIN":
        return erro(403, "apenas ADMIN gere usuarios",
                    event.get("rawPath", ""))

    rota = event.get("routeKey", "")
    caminho_url = event.get("rawPath", rota)
    try:
        corpo = json.loads(event.get("body") or "{}")
    except ValueError:
        return erro(400, "body invalido", caminho_url)

    if rota == "GET /v1/users":
        saida = []
        for u in idp.list_users(UserPoolId=POOL, Limit=60)["Users"]:
            grupos = idp.admin_list_groups_for_user(
                UserPoolId=POOL, Username=u["Username"]
            )["Groups"]
            nomes = [g["GroupName"] for g in grupos]
            papel = next((p for p in PAPEIS if p in nomes), "VIEWER")
            saida.append(molda(u, papel))
        return resp(200, saida)

    if rota == "POST /v1/users":
        email = corpo.get("email") or ""
        senha = corpo.get("password") or ""
        nome = (corpo.get("name") or "").strip()
        papel = corpo.get("role") or "VIEWER"
        if not email or not senha or not nome:
            return erro(400, "email, password e name obrigatorios", caminho_url)
        if papel not in PAPEIS:
            return erro(400, f"role: {'|'.join(PAPEIS)}", caminho_url,
                        {"role": "|".join(PAPEIS)})
        try:
            idp.admin_create_user(
                UserPoolId=POOL, Username=email, MessageAction="SUPPRESS",
                UserAttributes=[
                    {"Name": "email", "Value": email},
                    {"Name": "email_verified", "Value": "true"},
                    {"Name": "name", "Value": nome},
                ],
            )
        except idp.exceptions.UsernameExistsException:
            return erro(409, f"{email} ja existe", caminho_url)
        idp.admin_set_user_password(
            UserPoolId=POOL, Username=email, Password=senha, Permanent=True
        )
        idp.admin_add_user_to_group(
            UserPoolId=POOL, Username=email, GroupName=papel
        )
        u = idp.admin_get_user(UserPoolId=POOL, Username=email)
        return resp(201, molda(u, papel))

    return erro(404, "rota desconhecida", caminho_url)
