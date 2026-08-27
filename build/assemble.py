#!/usr/bin/env python3
"""Monta template.yaml a partir dos fragmentos em infra/ e do codigo fonte.

Por que isto existe: o Git sync do CloudFormation aplica UM arquivo de
template, exatamente como esta no repositorio. Ele nao roda
'aws cloudformation package', entao nao ha !Include nativo nem nested stack
sem antes subir o filho para o S3. O jeito de manter fontes pequenas e montar
o arquivo unico e commitar o resultado.

  infra/header.yaml        cabecalho e Description
  infra/parameters.yaml    corpo de Parameters
  infra/conditions.yaml    corpo de Conditions
  infra/resources/*.yaml   corpo de Resources, em ordem alfabetica do arquivo
  infra/outputs.yaml       corpo de Outputs

Os fragmentos sao concatenados como texto, e nao mesclados como objetos YAML,
para preservar os comentarios -- que aqui carregam o porque de varias
decisoes.

Dentro de um fragmento, a linha

    {{ include: caminho/arquivo }}

e substituida pelo conteudo do arquivo, recuado ate a coluna do marcador. E
assim que o script do Glue Job, os handlers das Lambdas e a pagina do
dashboard vivem como arquivos de verdade, lintaveis e testaveis, em vez de
blocos escalares dentro do YAML.

Uso:
    python3 build/assemble.py            # escreve template.yaml
    python3 build/assemble.py --check    # falha se o commitado estiver velho
"""

import re
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
INFRA = RAIZ / "infra"
DESTINO = RAIZ / "template.yaml"

AVISO = """# ATENCAO: arquivo GERADO por build/assemble.py -- nao edite aqui.
# As fontes estao em infra/ (fragmentos YAML), lambdas/, glue/ e frontend/.
# Depois de mexer em qualquer uma delas, rode: make build
"""

MARCADOR = re.compile(r"^(\s*)\{\{ include: (.+?) \}\}\s*$")


def expande(texto, origem):
    """Substitui os marcadores de include pelo conteudo dos arquivos."""
    saida = []
    for linha in texto.split("\n"):
        achado = MARCADOR.match(linha)
        if not achado:
            saida.append(linha)
            continue
        recuo, caminho = achado.group(1), achado.group(2)
        arquivo = RAIZ / caminho
        if not arquivo.exists():
            raise SystemExit(f"{origem}: include nao encontrado: {caminho}")
        for interna in arquivo.read_text().rstrip("\n").split("\n"):
            saida.append((recuo + interna).rstrip())
    return "\n".join(saida)


def secao(nome, cabecalho, arquivos):
    partes = [
        expande(arquivo.read_text().strip("\n"), arquivo.name)
        for arquivo in arquivos
    ]
    corpo = "\n\n".join(p for p in partes if p.strip())
    if not corpo:
        raise SystemExit(f"secao {nome} ficou vazia")
    return f"{cabecalho}\n\n{corpo}\n"


def monta():
    fragmentos = sorted((INFRA / "resources").glob("*.yaml"))
    if not fragmentos:
        raise SystemExit("nenhum fragmento em infra/resources/")
    return "\n".join([
        (INFRA / "header.yaml").read_text().strip("\n"),
        "",
        AVISO.rstrip("\n"),
        "",
        secao("Parameters", "Parameters:", [INFRA / "parameters.yaml"]),
        secao("Conditions", "Conditions:", [INFRA / "conditions.yaml"]),
        secao("Resources", "Resources:", fragmentos),
        secao("Outputs", "Outputs:", [INFRA / "outputs.yaml"]),
    ])


if __name__ == "__main__":
    montado = monta()
    if "--check" in sys.argv:
        atual = DESTINO.read_text() if DESTINO.exists() else ""
        if atual == montado:
            print("ok: template.yaml esta em dia com infra/, lambdas/, glue/ e frontend/")
            sys.exit(0)
        print(
            "ERRO: template.yaml esta desatualizado em relacao as fontes.\n"
            "Rode 'make build' (ou python3 build/assemble.py) e commite o resultado.",
            file=sys.stderr,
        )
        sys.exit(1)
    DESTINO.write_text(montado)
    print(f"template.yaml gerado: {len(montado)} bytes")
