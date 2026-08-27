#!/usr/bin/env python3
"""Valida a implementacao de geohash usada no Glue Job.

O codigo e extraido de glue/rollup_job.py por AST -- o script nao pode ser
importado direto porque faz `from awsglue... import` no topo, e um Glue Job
tem que ser um arquivo unico, entao mover o geohash para um modulo separado
nao e opcao. Extrair evita a alternativa ruim, que seria manter uma segunda
copia da funcao aqui.
"""

import ast
import math
import random
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
NOMES = {"geohash", "centro"}
CONSTANTES = {"BASE32"}

arvore = ast.parse((RAIZ / "glue/rollup_job.py").read_text())
trechos = [
    no for no in arvore.body
    if (isinstance(no, ast.FunctionDef) and no.name in NOMES)
    or (
        isinstance(no, ast.Assign)
        and any(getattr(a, "id", None) in CONSTANTES for a in no.targets)
    )
]
faltando = NOMES - {no.name for no in trechos if isinstance(no, ast.FunctionDef)}
if faltando:
    print(f"ERRO: nao achei em rollup_job.py: {faltando}", file=sys.stderr)
    sys.exit(1)

escopo = {}
exec(compile(ast.Module(body=trechos, type_ignores=[]), "rollup_job", "exec"), escopo)
geohash, centro = escopo["geohash"], escopo["centro"]

falhas = []


def checa(rotulo, obtido, esperado):
    ok = obtido == esperado
    print(f"{'OK  ' if ok else 'FALHA'} {rotulo}: {obtido}")
    if not ok:
        falhas.append(rotulo)


# Vetores da especificacao.
checa("vetor conhecido (57.64911, 10.40744) p11",
      geohash(57.64911, 10.40744, 11), "u4pruydqqvj")
checa("origem (0, 0) p6", geohash(0.0, 0.0, 6), "s00000")

# A propriedade de prefixo e o que faz o desenho funcionar: begins_with no
# DynamoDB recorta uma regiao do mapa porque a celula grossa prefixa a fina.
lat, lon = -23.5505, -46.6333
checa("p7 prefixa p9", geohash(lat, lon, 9).startswith(geohash(lat, lon, 7)), True)
checa("p5 prefixa p7", geohash(lat, lon, 7).startswith(geohash(lat, lon, 5)), True)

# Round-trip: o centro decodificado tem que cair dentro da celula de origem.
random.seed(11)
fora = 0
for _ in range(2000):
    a = random.uniform(-89.9, 89.9)
    b = random.uniform(-179.9, 179.9)
    for precisao in (7, 9):
        codigo = geohash(a, b, precisao)
        clat, clon, dlat, dlon = centro(codigo)
        if abs(clat - a) > dlat / 2 + 1e-9 or abs(clon - b) > dlon / 2 + 1e-9:
            fora += 1
checa("round-trip em 4000 pontos", fora, 0)

# Resolucao: a celula fina precisa ser menor que a largura do implemento
# (6 a 12 m), senao sobreposicao nunca aparece no heatmap.
_, _, dlat, dlon = centro(geohash(-23.5, -46.6, 9))
metros_lat = dlat * 111_320
metros_lon = dlon * 111_320 * math.cos(math.radians(-23.5))
print(f"     celula p9 a 23.5S: {metros_lat:.1f} m x {metros_lon:.1f} m")
checa("celula p9 menor que o implemento", max(metros_lat, metros_lon) < 6.0, True)

_, _, dlat, dlon = centro(geohash(-23.5, -46.6, 7))
metros = dlat * 111_320
print(f"     celula p7 a 23.5S: {metros:.1f} m")
checa("celula p7 entre 100 e 200 m", 100 < metros < 200, True)

# Deslocamentos na escala do trabalho agricola caem em celulas distintas.
checa("10 m separam celulas p9",
      geohash(-23.5000, -46.6000, 9) != geohash(-23.5000, -46.6001, 9), True)
checa("10 m NAO separam celulas p7",
      geohash(-23.5000, -46.6000, 7) == geohash(-23.5000, -46.6001, 7), True)

# O tamanho que a API informa ao cliente tem que bater com o real.
for precisao in (7, 9):
    bits = 5 * precisao
    esperado_lat = 180.0 / (2 ** (bits // 2))
    esperado_lon = 360.0 / (2 ** ((bits + 1) // 2))
    _, _, real_lat, real_lon = centro(geohash(-23.5, -46.6, precisao))
    checa(f"cellSize p{precisao} confere com o real",
          (round(esperado_lat, 12), round(esperado_lon, 12)),
          (round(real_lat, 12), round(real_lon, 12)))

print("\n=> todos os casos passaram" if not falhas else f"\n=> {len(falhas)} FALHA(S)")
sys.exit(1 if falhas else 0)
