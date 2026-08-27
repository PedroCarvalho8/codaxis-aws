#!/usr/bin/env python3
"""Garante que os arquivos embutidos no template continuam iguais aos originais.

Dois recursos carregam o conteudo de um arquivo do repositorio numa
propriedade do template:

  RollupScript   -> rollup_job.py        (script do Glue Job)
  FrontendIndex  -> frontend/index.html  (pagina do dashboard)

Os arquivos continuam existindo soltos para lint, editor e execucao/abertura
local. Como sao duas copias, elas podem divergir em silencio -- e o sintoma
seria o job rodando codigo velho, ou o site publicado nao batendo com o
repositorio. Esta checagem quebra o build antes disso.
"""

import sys
from pathlib import Path

import yaml


class Loader(yaml.SafeLoader):
    """SafeLoader que ignora as tags curtas do CloudFormation (!Ref, !Sub...)."""


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

PARES = [
    ("RollupScript", "rollup_job.py"),
    ("FrontendIndex", "frontend/index.html"),
]

raiz = Path(__file__).resolve().parent.parent
template = yaml.load((raiz / "template.yaml").read_text(), Loader=Loader)

falhas = []
for recurso, caminho in PARES:
    embutido = template["Resources"][recurso]["Properties"]["Content"]
    arquivo = (raiz / caminho).read_text()
    if embutido == arquivo:
        print(f"ok: {recurso}.Content == {caminho}")
    else:
        falhas.append((recurso, caminho))
        print(f"ERRO: {recurso}.Content divergiu de {caminho}", file=sys.stderr)

if falhas:
    print(
        "\nReaplique o conteudo do arquivo na propriedade Content do recurso,\n"
        "preservando o recuo do bloco escalar.",
        file=sys.stderr,
    )
    sys.exit(1)
