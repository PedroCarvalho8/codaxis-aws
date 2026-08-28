#!/usr/bin/env python3
"""Exercita os handlers de gestao (authorizer, leitura e escrita) sem AWS.

Como nos outros testes, o codigo vem de dentro do template.yaml gerado -- o
que se testa e o artefato que a pilha implanta. O cliente IoT e um stub que
grava a ORDEM das chamadas, porque numa revogacao a ordem e o que protege:
desativar o certificado vem antes de qualquer passo que possa falhar.
"""

import importlib.util
import json
import os
import sys
import tempfile
import types
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

    class ParameterNotFound(Exception):
        pass


class IotFake:
    exceptions = Excecoes

    def __init__(self):
        self.chamadas = []
        self.things = {"trator-01": ["arn:aws:iot:r:1:cert/aaa111"]}

    def _grava(self, nome, **kw):
        self.chamadas.append((nome, kw))

    def create_thing(self, thingName):
        self._grava("create_thing", thingName=thingName)
        if thingName in self.things:
            raise Excecoes.ResourceAlreadyExistsException()
        self.things[thingName] = []

    def describe_thing(self, thingName):
        self._grava("describe_thing", thingName=thingName)
        if thingName not in self.things:
            raise Excecoes.ResourceNotFoundException()
        return {"thingName": thingName}

    def delete_thing(self, thingName):
        self._grava("delete_thing", thingName=thingName)
        self.things.pop(thingName)

    def list_thing_principals(self, thingName):
        if thingName not in self.things:
            raise Excecoes.ResourceNotFoundException()
        return {"principals": list(self.things[thingName])}

    def create_keys_and_certificate(self, setAsActive):
        self._grava("create_keys_and_certificate", setAsActive=setAsActive)
        return {
            "certificateArn": "arn:aws:iot:r:1:cert/novo999",
            "certificateId": "novo999",
            "certificatePem": "PEM",
            "keyPair": {"PrivateKey": "PRIVADA"},
        }

    def describe_certificate(self, certificateId):
        import datetime
        return {"certificateDescription": {
            "status": "ACTIVE",
            "creationDate": datetime.datetime(2026, 8, 27, 12, 0),
        }}

    def attach_policy(self, policyName, target):
        self._grava("attach_policy", policyName=policyName, target=target)

    def detach_policy(self, policyName, target):
        self._grava("detach_policy", policyName=policyName, target=target)

    def attach_thing_principal(self, thingName, principal):
        self._grava("attach_thing_principal", thingName=thingName)
        self.things[thingName].append(principal)

    def detach_thing_principal(self, thingName, principal):
        self._grava("detach_thing_principal", thingName=thingName)

    def update_certificate(self, certificateId, newStatus):
        self._grava("update_certificate", certificateId=certificateId,
                    newStatus=newStatus)

    def delete_certificate(self, certificateId):
        self._grava("delete_certificate", certificateId=certificateId)

    def describe_endpoint(self, endpointType):
        return {"endpointAddress": "abc-ats.iot.us-east-1.amazonaws.com"}

    def list_things(self, maxResults, **kw):
        return {"things": [{"thingName": n} for n in sorted(self.things)]}


class SsmFake:
    exceptions = Excecoes

    def __init__(self):
        self.valor = None

    def get_parameter(self, Name, WithDecryption):
        if self.valor is None:
            raise Excecoes.ParameterNotFound()
        return {"Parameter": {"Value": self.valor}}


iot_fake = IotFake()
ssm_fake = SsmFake()
boto3.client = lambda nome, **kw: {"iot": iot_fake, "ssm": ssm_fake}[nome]


def carrega(recurso):
    raiz = Path(__file__).resolve().parent.parent
    template = yaml.load((raiz / "template.yaml").read_text(), Loader=Loader)
    codigo = template["Resources"][recurso]["Properties"]["Code"]["ZipFile"]
    os.environ.setdefault("ADMIN_KEY_PARAM", "/teste/admin/api-key")
    os.environ.setdefault("DEVICE_POLICY", "politica-frota")
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(codigo)
        caminho = f.name
    spec = importlib.util.spec_from_file_location("h_" + recurso, caminho)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


authorizer = carrega("AdminAuthorizerFunction").handler
leitura = carrega("AdminReadFunction").handler
escrita = carrega("AdminWriteFunction").handler

falhas = []


def checa(rotulo, obtido, esperado):
    ok = obtido == esperado
    print(f"{'OK  ' if ok else 'FALHA'} {rotulo}: {obtido!r}"[:120])
    if not ok:
        falhas.append(rotulo)


def evento(rota, caminho=None, corpo=None):
    return {"routeKey": rota, "pathParameters": caminho or {},
            "body": json.dumps(corpo) if corpo else None}


# ------------------------------------------------------------- authorizer
checa("auth: sem parametro no SSM -> nega",
      authorizer({"headers": {"x-api-key": "qualquer"}}, None),
      {"isAuthorized": False})
ssm_fake.valor = "segredo-123"
checa("auth: chave certa apos criar o parametro (sem redeploy)",
      authorizer({"headers": {"x-api-key": "segredo-123"}}, None),
      {"isAuthorized": True})
checa("auth: chave errada -> nega",
      authorizer({"headers": {"x-api-key": "errada"}}, None),
      {"isAuthorized": False})
ssm_fake.valor = "segredo-456"
checa("auth: chave rotacionada vale sem redeploy",
      authorizer({"headers": {"x-api-key": "segredo-456"}}, None),
      {"isAuthorized": True})

# ---------------------------------------------------------------- leitura
r = leitura(evento("GET /admin/devices"), None)
checa("read: lista devices", json.loads(r["body"])["devices"], ["trator-01"])
r = leitura(evento("GET /admin/devices/{device_id}",
                   {"device_id": "trator-01"}), None)
corpo = json.loads(r["body"])
checa("read: detalhe traz endpoint",
      corpo["endpoint"].endswith("amazonaws.com"), True)
checa("read: detalhe traz certificados",
      corpo["certificates"][0]["certificate_id"], "aaa111")
checa("read: device inexistente -> 404",
      leitura(evento("GET /admin/devices/{device_id}",
                     {"device_id": "nao-existe"}), None)["statusCode"], 404)

# ---------------------------------------------------------------- escrita
r = escrita(evento("POST /admin/devices", corpo={"device_id": "trator-02"}), None)
corpo = json.loads(r["body"])
checa("write: criacao -> 201", r["statusCode"], 201)
checa("write: devolve a chave privada uma unica vez",
      corpo["private_key"], "PRIVADA")
checa("write: anexa policy e thing na ordem",
      [c[0] for c in iot_fake.chamadas if c[0].startswith("attach")],
      ["attach_policy", "attach_thing_principal"])
checa("write: nome invalido -> 400",
      escrita(evento("POST /admin/devices",
                     corpo={"device_id": "com espaco!"}), None)["statusCode"], 400)
checa("write: duplicado -> 409",
      escrita(evento("POST /admin/devices",
                     corpo={"device_id": "trator-01"}), None)["statusCode"], 409)

iot_fake.chamadas = []
r = escrita(evento("DELETE /admin/devices/{device_id}/certificates/{certificate_id}",
                   {"device_id": "trator-01", "certificate_id": "aaa111"}), None)
checa("write: revogacao -> 200", r["statusCode"], 200)
checa("write: revogacao desativa ANTES de desanexar e apagar",
      [c[0] for c in iot_fake.chamadas],
      ["update_certificate", "detach_policy", "detach_thing_principal",
       "delete_certificate"])
checa("write: desativacao veio com INACTIVE",
      iot_fake.chamadas[0][1]["newStatus"], "INACTIVE")

checa("write: cert de outro device -> 404",
      escrita(evento("PATCH /admin/devices/{device_id}/certificates/{certificate_id}",
                     {"device_id": "trator-02", "certificate_id": "aaa111"},
                     corpo={"status": "INACTIVE"}), None)["statusCode"], 404)

iot_fake.chamadas = []
r = escrita(evento("DELETE /admin/devices/{device_id}",
                   {"device_id": "trator-02"}), None)
checa("write: descomissionar device -> 200", r["statusCode"], 200)
checa("write: teardown revoga certs e apaga o thing por ultimo",
      [c[0] for c in iot_fake.chamadas][-1], "delete_thing")

print("\n=> todos os casos passaram" if not falhas else f"\n=> {len(falhas)} FALHA(S)")
sys.exit(1 if falhas else 0)
