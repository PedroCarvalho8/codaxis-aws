import json
import os

import boto3

idp = boto3.client("cognito-idp")
CLIENT_ID = os.environ["CLIENT_ID"]


def resp(status, corpo):
    return {"statusCode": status,
            "headers": {"content-type": "application/json",
                        "cache-control": "no-store"},
            "body": json.dumps(corpo)}


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
        return resp(400, {"message": "body invalido"})
    rota = event.get("routeKey", "")

    try:
        if rota == "POST /auth/login":
            email = corpo.get("email") or ""
            senha = corpo.get("password") or ""
            if not email or not senha:
                return resp(400, {"message": "email e password obrigatorios"})
            r = idp.initiate_auth(
                ClientId=CLIENT_ID,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": email, "PASSWORD": senha},
            )
            if "AuthenticationResult" not in r:
                # NEW_PASSWORD_REQUIRED etc.: fluxo de desafio nao suportado
                # pela API; o operador finaliza o cadastro pela CLI.
                return resp(409, {"message": f"desafio pendente: {r.get('ChallengeName')}"})
            return resp(200, tokens(r))

        if rota == "POST /auth/refresh":
            atual = corpo.get("refreshToken") or ""
            if not atual:
                return resp(400, {"message": "refreshToken obrigatorio"})
            r = idp.initiate_auth(
                ClientId=CLIENT_ID,
                AuthFlow="REFRESH_TOKEN_AUTH",
                AuthParameters={"REFRESH_TOKEN": atual},
            )
            return resp(200, tokens(r, refresh_atual=atual))
    except (idp.exceptions.NotAuthorizedException,
            idp.exceptions.UserNotFoundException):
        return resp(401, {"message": "credenciais invalidas"})

    return resp(404, {"message": "rota desconhecida"})
