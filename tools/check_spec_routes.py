#!/usr/bin/env python3
"""Falha se as rotas do gateway divergirem de openapi/openapi.yaml.

A spec e a fonte do contrato; os handlers implementam. Esta checagem impede
o modo de falha silencioso: rota nova sem spec, ou spec prometendo rota que
nao existe.
"""
import sys
from pathlib import Path

import yaml


class Loader(yaml.SafeLoader):
    pass


Loader.add_multi_constructor(
    "!",
    lambda l, s, n: (
        l.construct_scalar(n) if isinstance(n, yaml.ScalarNode)
        else l.construct_sequence(n) if isinstance(n, yaml.SequenceNode)
        else l.construct_mapping(n)
    ),
)

raiz = Path(__file__).resolve().parent.parent
template = yaml.load((raiz / "template.yaml").read_text(), Loader=Loader)
implementadas = {
    r["Properties"]["RouteKey"]
    for r in template["Resources"].values()
    if r.get("Type") == "AWS::ApiGatewayV2::Route"
}
spec = yaml.safe_load((raiz / "openapi/openapi.yaml").read_text())
da_spec = {
    f"{metodo.upper()} {path}"
    for path, ops in spec["paths"].items()
    for metodo in ops
}
faltam = da_spec - implementadas
sobram = implementadas - da_spec
if faltam or sobram:
    if faltam:
        print(f"ERRO: na spec, sem rota no gateway: {sorted(faltam)}", file=sys.stderr)
    if sobram:
        print(f"ERRO: rota no gateway fora da spec: {sorted(sobram)}", file=sys.stderr)
    sys.exit(1)
print(f"ok: {len(da_spec)} rotas batem 1:1 com a spec")
