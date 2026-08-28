import json
from datetime import datetime, timezone
from http.client import responses
import os

import boto3

idp = boto3.client("cognito-idp")
CLIENT_ID = os.environ["CLIENT_ID"]


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


def tokens(resultado, refresh_atual=None):
    """Molda a resposta no TokenResponse do contrato argus.

    O accessToken e o ID TOKEN do Cognito de proposito: e ele que carrega
    email, aud e o claim 'role' que o pre-token-generation injeta -- e o que
    o webapp decodifica. O access token do Cognito nao aceita claim custom
    sem feature plan pago.
    """
    r = resultado["AuthenticationResult"]
    return {
        "accessToken": r["IdToken"],
        # No refresh o Cognito nao devolve novo refresh token; o atual segue
        # valendo ate expirar.
        "refreshToken": r.get("RefreshToken") or refresh_atual,
        "expiresIn": r["ExpiresIn"],
    }


def handler(event, context):
    try:
        corpo = json.loads(event.get("body") or "{}")
    except ValueError:
        return erro(400, "body invalido", event.get("rawPath", ""))
    rota = event.get("routeKey", "")
    caminho = event.get("rawPath", rota)

    try:
        if rota == "POST /v1/auth/login":
            email = corpo.get("email") or ""
            senha = corpo.get("password") or ""
            if not email or not senha:
                return erro(400, "email e password obrigatorios", caminho)
            r = idp.initiate_auth(
                ClientId=CLIENT_ID,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": email, "PASSWORD": senha},
            )
            if "AuthenticationResult" not in r:
                # NEW_PASSWORD_REQUIRED etc.: fluxo de desafio nao suportado
                # pela API; o operador finaliza o cadastro pela CLI.
                return erro(409, f"desafio pendente: {r.get('ChallengeName')}", caminho)
            return resp(200, tokens(r))

        if rota == "POST /v1/auth/refresh":
            atual = corpo.get("refreshToken") or ""
            if not atual:
                return erro(400, "refreshToken obrigatorio", caminho)
            r = idp.initiate_auth(
                ClientId=CLIENT_ID,
                AuthFlow="REFRESH_TOKEN_AUTH",
                AuthParameters={"REFRESH_TOKEN": atual},
            )
            return resp(200, tokens(r, refresh_atual=atual))
    except (idp.exceptions.NotAuthorizedException,
            idp.exceptions.UserNotFoundException):
        return erro(401, "credenciais invalidas", caminho)

    return erro(404, "rota desconhecida", caminho)
