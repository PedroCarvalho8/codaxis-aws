#!/usr/bin/env python3
"""Exercita os handlers do contrato argus sem AWS.

O codigo vem de dentro do template.yaml gerado -- o que se testa e o artefato
que a pilha implanta. AWS e substituida por stubs; o do IoT grava a ORDEM das
chamadas, porque na revogacao a ordem e o que protege.

A validacao do token nao aparece aqui: e o JWT authorizer nativo do gateway.
O que aparece e o que E nosso: o gate de ADMIN em /api/users e o claim role
que o pre-token-generation injeta.
"""

import importlib.util
import json
import os
import sys
import tempfile
import types
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import boto3
import yaml


class Loader(yaml.SafeLoader):
    """SafeLoader que ignora as tags curtas do CloudFormation."""


Loader.add_multi_constructor(
    "!",
    lambda loader, suffix, node: (
        loader.construct_scalar(node)
        if isinstance(node, yaml.ScalarNode)
        else loader.construct_sequence(node)
        if isinstance(node, yaml.SequenceNode)
        else loader.construct_mapping(node)
    ),
)


class Excecoes:
    class ResourceNotFoundException(Exception):
        pass

    class ResourceAlreadyExistsException(Exception):
        pass

    class NotAuthorizedException(Exception):
        pass

    class UserNotFoundException(Exception):
        pass

    class UsernameExistsException(Exception):
        pass


class IotFake:
    exceptions = Excecoes

    def __init__(self):
        self.chamadas = []
        self.things = {}

    def _grava(self, nome, **kw):
        self.chamadas.append((nome, kw))

    def create_thing(self, thingName):
        self._grava("create_thing")
        if thingName in self.things:
            raise Excecoes.ResourceAlreadyExistsException()
        self.things[thingName] = []

    def create_keys_and_certificate(self, setAsActive):
        self._grava("create_keys_and_certificate")
        return {"certificateArn": "arn:aws:iot:r:1:cert/c1",
                "certificateId": "c1", "certificatePem": "CERT-PEM",
                "keyPair": {"PrivateKey": "KEY-PEM"}}

    def attach_policy(self, policyName, target):
        self._grava("attach_policy")

    def detach_policy(self, policyName, target):
        self._grava("detach_policy")

    def attach_thing_principal(self, thingName, principal):
        self._grava("attach_thing_principal")
        self.things[thingName].append(principal)

    def detach_thing_principal(self, thingName, principal):
        self._grava("detach_thing_principal")

    def list_thing_principals(self, thingName):
        if thingName not in self.things:
            raise Excecoes.ResourceNotFoundException()
        return {"principals": list(self.things[thingName])}

    def update_certificate(self, certificateId, newStatus):
        self._grava("update_certificate", newStatus=newStatus)

    def delete_certificate(self, certificateId):
        self._grava("delete_certificate")

    def describe_endpoint(self, endpointType):
        return {"endpointAddress": "abc-ats.iot.us-east-1.amazonaws.com"}


class TabelaFake:
    def __init__(self):
        self.itens = {}

    def put_item(self, Item):
        self.itens[(Item["pk"], Item["sk"])] = Item

    def get_item(self, Key):
        item = self.itens.get((Key["pk"], Key["sk"]))
        return {"Item": item} if item else {}

    def query(self, KeyConditionExpression, ScanIndexForward=True,
              Limit=None, **kw):
        # Suficiente para os padroes usados: eq(pk) [+ range no sk].
        valores, operadores = [], []

        def extrai(cond):
            operadores.append(cond.get_expression()["operator"])
            for v in getattr(cond, "_values", ()):
                if hasattr(v, "_values"):
                    extrai(v)
                elif isinstance(v, str):
                    valores.append(v)
        extrai(KeyConditionExpression)
        texto = " ".join(operadores)
        pk = valores[0]
        linhas = sorted(
            (i for (p, s), i in self.itens.items() if p == pk),
            key=lambda i: i["sk"], reverse=not ScanIndexForward,
        )
        resto = valores[1:]
        if "BETWEEN" in texto:
            a, b = resto
            linhas = [l for l in linhas if a <= l["sk"] <= b]
        elif resto and "<" in texto:
            linhas = [l for l in linhas if l["sk"] < resto[0]]
        elif resto:
            linhas = [l for l in linhas if l["sk"] >= resto[0]]
        corte = linhas[:Limit] if Limit else linhas
        saida = {"Items": [dict(i) for i in corte]}
        if Limit and len(linhas) > Limit:
            saida["LastEvaluatedKey"] = {"pk": pk, "sk": corte[-1]["sk"]}
        return saida


class IdpFake:
    exceptions = Excecoes

    def __init__(self):
        self.usuarios = {}
        self.grupos = {}
        self.senha_boa = ("admin@x.com", "senha-certa-123")

    def initiate_auth(self, ClientId, AuthFlow, AuthParameters):
        if AuthFlow == "USER_PASSWORD_AUTH":
            par = (AuthParameters["USERNAME"], AuthParameters["PASSWORD"])
            if par != self.senha_boa:
                raise Excecoes.NotAuthorizedException()
            return {"AuthenticationResult": {
                "IdToken": "ID.TOKEN", "RefreshToken": "REFRESH",
                "ExpiresIn": 3600}}
        return {"AuthenticationResult": {
            "IdToken": "ID.NOVO", "ExpiresIn": 3600}}

    def list_users(self, UserPoolId, Limit):
        return {"Users": list(self.usuarios.values())}

    def admin_list_groups_for_user(self, UserPoolId, Username):
        return {"Groups": [{"GroupName": g}
                           for g in self.grupos.get(Username, [])]}

    def admin_create_user(self, UserPoolId, Username, MessageAction,
                          UserAttributes):
        if Username in self.usuarios:
            raise Excecoes.UsernameExistsException()
        self.usuarios[Username] = {
            "Username": Username, "Enabled": True,
            "UserCreateDate": datetime(2026, 8, 28, 12, 0),
            "Attributes": [{"Name": "sub", "Value": f"sub-{Username}"}]
            + UserAttributes,
        }

    def admin_set_user_password(self, **kw):
        pass

    def admin_add_user_to_group(self, UserPoolId, Username, GroupName):
        self.grupos.setdefault(Username, []).append(GroupName)

    def admin_get_user(self, UserPoolId, Username):
        u = dict(self.usuarios[Username])
        u["UserAttributes"] = u.pop("Attributes")
        return u


iot_fake, tabela_fake, idp_fake = IotFake(), TabelaFake(), IdpFake()
boto3.client = lambda nome, **kw: {"iot": iot_fake,
                                   "cognito-idp": idp_fake}[nome]
boto3.resource = lambda *a, **kw: types.SimpleNamespace(
    Table=lambda n: tabela_fake)


def carrega(recurso):
    raiz = Path(__file__).resolve().parent.parent
    template = yaml.load((raiz / "template.yaml").read_text(), Loader=Loader)
    codigo = template["Resources"][recurso]["Properties"]["Code"]["ZipFile"]
    os.environ.setdefault("DEVICE_POLICY", "politica")
    os.environ.setdefault("TABLE", "t")
    os.environ.setdefault("CLIENT_ID", "client-1")
    os.environ.setdefault("USER_POOL", "pool-1")
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(codigo)
        caminho = f.name
    spec = importlib.util.spec_from_file_location("h_" + recurso, caminho)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


auth = carrega("AuthFunction").handler
dev_read = carrega("DevicesReadFunction").handler
dev_write = carrega("DevicesWriteFunction").handler
locs = carrega("LocationsFunction").handler
users = carrega("UsersFunction").handler
pretoken = carrega("PreTokenFunction").handler

falhas = []


def checa(rotulo, obtido, esperado):
    ok = obtido == esperado
    print(f"{'OK  ' if ok else 'FALHA'} {rotulo}: {obtido!r}"[:110])
    if not ok:
        falhas.append(rotulo)


def evento(rota, caminho=None, corpo=None, q=None, papel=None):
    e = {"routeKey": rota, "pathParameters": caminho or {},
         "queryStringParameters": q or {},
         "body": json.dumps(corpo) if corpo else None}
    if papel:
        e["requestContext"] = {"authorizer": {"jwt": {"claims":
                                                      {"role": papel}}}}
    return e


# -------------------------------------------------------------------- auth
r = auth(evento("POST /v1/auth/login",
                corpo={"email": "admin@x.com", "password": "senha-certa-123"}), None)
corpo = json.loads(r["body"])
checa("auth: login -> TokenResponse",
      sorted(corpo), ["accessToken", "expiresIn", "refreshToken"])
checa("auth: accessToken e o ID token", corpo["accessToken"], "ID.TOKEN")
checa("auth: senha errada -> 401",
      auth(evento("POST /v1/auth/login",
                  corpo={"email": "admin@x.com", "password": "x"}), None)["statusCode"], 401)
r = auth(evento("POST /v1/auth/refresh", corpo={"refreshToken": "REFRESH"}), None)
corpo = json.loads(r["body"])
checa("auth: refresh reusa o refresh token", corpo["refreshToken"], "REFRESH")

# ---------------------------------------------------------------- pretoken
saida = pretoken({"request": {"groupConfiguration":
                              {"groupsToOverride": ["VIEWER", "ADMIN"]}}}, None)
checa("pretoken: pega o papel mais forte",
      saida["response"]["claimsOverrideDetails"]["claimsToAddOrOverride"],
      {"role": "ADMIN"})
saida = pretoken({"request": {}}, None)
checa("pretoken: sem grupo vira VIEWER",
      saida["response"]["claimsOverrideDetails"]["claimsToAddOrOverride"],
      {"role": "VIEWER"})

# ----------------------------------------------------------------- devices
r = dev_write(evento("POST /v1/devices",
                     corpo={"code": "trator-07", "label": "John Deere 6110"}), None)
corpo = json.loads(r["body"])
checa("devices: criacao -> 201", r["statusCode"], 201)
checa("devices: DeviceCreated com device e credentials",
      sorted(corpo), ["credentials", "device"])
checa("devices: label com espaco preservado",
      corpo["device"]["label"], "John Deere 6110")
cred = corpo["credentials"]
checa("devices: credenciais estruturadas",
      (cred["privateKeyPem"], cred["certificatePem"], cred["clientId"]),
      ("KEY-PEM", "CERT-PEM", "trator-07"))
checa("devices: endpoint e topicos nas credenciais",
      (cred["mqttEndpoint"].endswith("amazonaws.com"),
       cred["topics"]["position"]), (True, "devices/trator-07/position"))
checa("devices: ativo ao nascer", corpo["device"]["active"], True)
checa("devices: duplicado -> 409",
      dev_write(evento("POST /v1/devices",
                       corpo={"code": "trator-07", "label": "x"}), None)["statusCode"], 409)

lista = json.loads(dev_read(evento("GET /v1/devices"), None)["body"])
checa("devices: lista no formato DeviceResponse",
      [d["code"] for d in lista], ["trator-07"])
um = json.loads(dev_read(evento("GET /v1/devices/{code}",
                                {"code": "trator-07"}), None)["body"])
checa("devices: detalhe", um["code"], "trator-07")
checa("devices: inexistente -> 404",
      dev_read(evento("GET /v1/devices/{code}",
                      {"code": "nao-ha"}), None)["statusCode"], 404)

iot_fake.chamadas = []
r = dev_write(evento("DELETE /v1/devices/{code}/credentials",
                     {"code": "trator-07"}), None)
corpo = json.loads(r["body"])
checa("devices: revogacao desativa ANTES de desanexar e apagar",
      [c[0] for c in iot_fake.chamadas],
      ["update_certificate", "detach_policy", "detach_thing_principal",
       "delete_certificate"])
checa("devices: revogado fica inativo com revokedAt",
      (corpo["active"], corpo["revokedAt"] is not None), (False, True))

# --------------------------------------------------------------- locations
tabela_fake.put_item(Item={"pk": "DEV#trator-07#position", "sk": "LATEST",
                           "ts": "2026-08-28T12:00:00Z",
                           "lat": Decimal("-21.17"), "lon": Decimal("-47.81"),
                           "speed": Decimal("7.2"), "heading": Decimal("90"),
                           "received_at": "2026-08-28T12:00:03Z"})
r = json.loads(locs(evento("GET /v1/locations/latest"), None)["body"])
checa("locations: latest no formato LocationResponse",
      (r[0]["deviceCode"], r[0]["lat"], r[0]["speedMps"], r[0]["courseDeg"]),
      ("trator-07", -21.17, 2.0, 90.0))
checa("locations: receivedAt preservado", r[0]["receivedAt"], "2026-08-28T12:00:03Z")

tabela_fake.put_item(Item={"pk": "DEVICEMETA", "sk": "trator-07",
                           "label": "x", "active": True})
for i in range(5):
    tabela_fake.put_item(Item={"pk": "TRACK#trator-07",
                               "sk": f"2026-08-28T12:00:{i:02d}Z#00000{i}",
                               "ts": f"2026-08-28T12:00:{i:02d}Z",
                               "lat": Decimal("-21.17"), "lon": Decimal("-47.81")})
r = json.loads(locs(evento("GET /v1/devices/{code}/locations",
                           caminho={"code": "trator-07"},
                           q={"limit": "2"}), None)["body"])
checa("locations: pagina mais recente primeiro",
      [i["ts"][-3:-1] for i in r["items"]], ["04", "03"])
checa("locations: cursor keyset presente", r["nextCursor"] is not None, True)
r2 = json.loads(locs(evento("GET /v1/devices/{code}/locations",
                            caminho={"code": "trator-07"},
                            q={"limit": "2", "to": r["nextCursor"]}), None)["body"])
checa("locations: pagina seguinte nao repete o cursor",
      [i["ts"][-3:-1] for i in r2["items"]], ["02", "01"])
checa("locations: device desconhecido -> 404",
      locs(evento("GET /v1/devices/{code}/locations",
                  caminho={"code": "fantasma"}), None)["statusCode"], 404)

# ------------------------------------------------------------------- users
checa("users: sem ADMIN -> 403",
      users(evento("GET /v1/users", papel="OPERATOR"), None)["statusCode"], 403)
r = users(evento("POST /v1/users", papel="ADMIN",
                 corpo={"email": "op@x.com", "password": "SenhaForte-123",
                        "name": "Operadora", "role": "OPERATOR"}), None)
corpo = json.loads(r["body"])
checa("users: criacao -> 201 no formato UserResponse",
      (corpo["email"], corpo["name"], corpo["role"]),
      ("op@x.com", "Operadora", "OPERATOR"))
lista = json.loads(users(evento("GET /v1/users", papel="ADMIN"), None)["body"])
checa("users: lista com papel resolvido do grupo",
      [(u["email"], u["role"]) for u in lista], [("op@x.com", "OPERATOR")])
r = users(evento("POST /v1/users", papel="ADMIN",
                 corpo={"email": "a@x.com", "password": "SenhaForte-123",
                        "name": "A", "role": "ROOT"}), None)
corpo = json.loads(r["body"])
checa("users: papel invalido -> 400", r["statusCode"], 400)
checa("erros: ApiError completo da spec",
      sorted(corpo), ["error", "fields", "message", "path", "status", "timestamp"])
checa("erros: error nomeado pela stdlib", corpo["error"], "Bad Request")
checa("erros: fields aponta o campo", "role" in corpo["fields"], True)

print("\n=> todos os casos passaram" if not falhas else f"\n=> {len(falhas)} FALHA(S)")
sys.exit(1 if falhas else 0)
