#!/usr/bin/env python3
"""Garante que o script embutido no template e o rollup_job.py sao identicos.

O custom resource RollupScript carrega o conteudo do job numa propriedade do
template. Como rollup_job.py continua existindo como arquivo (para lint,
editor e execucao local), as duas copias podem divergir sem ninguem notar --
e o sintoma seria o job rodando codigo velho.
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

raiz = Path(__file__).resolve().parent.parent
template = yaml.load((raiz / "template.yaml").read_text(), Loader=Loader)
embutido = template["Resources"]["RollupScript"]["Properties"]["Content"]
arquivo = (raiz / "rollup_job.py").read_text()

if embutido == arquivo:
    print("ok: RollupScript.Content == rollup_job.py")
    sys.exit(0)

print(
    "ERRO: o script embutido no template divergiu de rollup_job.py.\n"
    "Reaplique o conteudo do arquivo na propriedade Content do recurso\n"
    "RollupScript, preservando o recuo do bloco escalar.",
    file=sys.stderr,
)
sys.exit(1)
