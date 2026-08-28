import hmac
import os

import boto3

ssm = boto3.client("ssm")
PARAM = os.environ["ADMIN_KEY_PARAM"]
_cache = {}


def chave():
    if "v" not in _cache:
        try:
            _cache["v"] = ssm.get_parameter(Name=PARAM, WithDecryption=True)[
                "Parameter"]["Value"]
        except ssm.exceptions.ParameterNotFound:
            # Sem o parametro no SSM, a API de gestao fica DESLIGADA --
            # nunca aberta por omissao.
            _cache["v"] = None
    return _cache["v"]


def handler(event, context):
    dado = (event.get("headers") or {}).get("x-api-key") or ""
    alvo = chave()
    if alvo and hmac.compare_digest(dado, alvo):
        return {"isAuthorized": True}
    # Recarrega uma vez: cobre rotacao da chave sem redeploy.
    _cache.pop("v", None)
    alvo = chave()
    return {"isAuthorized": bool(alvo) and hmac.compare_digest(dado, alvo)}
