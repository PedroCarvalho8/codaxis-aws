#!/usr/bin/env python3
"""Gera (e opcionalmente publica) massa de posicao de tratores para o pipeline.

O cliente de teste do console publica uma mensagem por vez, e a 1 fix a cada
5 s duas horas de trabalho dao 1440 pontos por trator -- inviavel na mao. Este
script monta a rota, quebra em lotes dentro dos limites do IoT Core e publica.

A rota nao e ruido aleatorio: e serpentina de passadas paralelas num talhao
retangular, com manobra de cabeceira (onde o trator desacelera e a permanencia
se concentra), uma faixa repassada de proposito para a sobreposicao aparecer no
heatmap, e uma parada longa de motor desligado -- que existe para exercitar o
teto de dt: sem ele, a parada doaria horas para a celula onde o trator ficou.

  # so gerar, para inspecionar antes de mandar
  python3 tools/simula_posicoes.py --saida /tmp/posicoes.jsonl

  # gerar e publicar no IoT Core
  python3 tools/simula_posicoes.py --publicar --regiao us-east-1

  # basic ingest: publica direto no Rules Engine, sem cobranca de broker
  python3 tools/simula_posicoes.py --publicar --topico '$aws/rules/iot_telemetry_position'
"""

import argparse
import json
import math
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

# Limites do caminho IoT Core -> Firehose.
MAX_ELEMENTOS = 500      # teto do PutRecordBatch, com o BatchMode da regra
MAX_PAYLOAD = 128 * 1024  # teto do payload MQTT

METROS_POR_GRAU_LAT = 111_320.0


def desloca(lat, lon, norte_m, leste_m):
    """Soma um deslocamento em metros a uma coordenada."""
    d_lat = norte_m / METROS_POR_GRAU_LAT
    d_lon = leste_m / (METROS_POR_GRAU_LAT * math.cos(math.radians(lat)))
    return lat + d_lat, lon + d_lon


def rota(base_lat, base_lon, inicio, args, aleatorio):
    """Serpentina de passadas paralelas, com cabeceira, repasse e parada.

    Devolve a lista de fixes em ordem cronologica.
    """
    fixes = []
    agora = inicio
    comprimento = args.comprimento
    largura_implemento = args.implemento
    velocidade = args.velocidade / 3.6          # km/h -> m/s

    passada = 0
    # Faixas que o trator repassa, para sobreposicao aparecer no mapa fino.
    repasses = {3, 4}
    fim = inicio + timedelta(hours=args.horas)
    parada_em = inicio + timedelta(hours=args.horas * 0.55) if args.parada else None
    ja_parou = False

    while agora < fim:
        avanco = 0.0
        norte = passada * largura_implemento
        sentido = 1 if passada % 2 == 0 else -1

        while avanco < comprimento and agora < fim:
            # Cabeceira: o trator desacelera para manobrar nas pontas.
            perto_da_ponta = avanco < 25 or avanco > comprimento - 25
            v = velocidade * (0.28 if perto_da_ponta else 1.0)
            v *= aleatorio.uniform(0.92, 1.08)

            leste = avanco if sentido == 1 else comprimento - avanco
            # Ruido de GPS: alguns metros, como um receptor real.
            lat, lon = desloca(
                base_lat,
                base_lon,
                norte + aleatorio.gauss(0, args.ruido),
                leste + aleatorio.gauss(0, args.ruido),
            )
            fixes.append({
                "event_time": agora.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "lat": round(lat, 7),
                "lon": round(lon, 7),
                "speed": round(v * 3.6, 2),
                "heading": 90 if sentido == 1 else 270,
            })

            agora += timedelta(seconds=args.intervalo)
            avanco += v * args.intervalo

            # Parada longa de motor desligado: nenhum fix durante o intervalo.
            # O teto de dt no job impede que isso vire uma celula quente falsa.
            if parada_em and not ja_parou and agora >= parada_em:
                agora += timedelta(minutes=args.parada)
                ja_parou = True

        passada += 1
        if passada in repasses and args.repasse:
            passada -= 1
            repasses.discard(passada + 1)

    return fixes


def em_lotes(fixes, device_id, maximo):
    """Quebra os fixes em mensagens MQTT dentro dos limites do IoT Core."""
    mensagens = []
    for comeco in range(0, len(fixes), maximo):
        pedaco = fixes[comeco:comeco + maximo]
        lote = uuid.uuid4().hex[:8]
        leituras = [
            # seq e unico no LOTE INTEIRO, nao por campo: o job deduplica por
            # (device_id, batch_id, seq), entao repetir seq descartaria fixes.
            dict(fix, device_id=device_id, batch_id=f"b-{lote}", seq=i)
            for i, fix in enumerate(pedaco)
        ]
        corpo = json.dumps({"positions": leituras}, separators=(",", ":"))
        if len(corpo.encode("utf-8")) > MAX_PAYLOAD:
            # Nunca deveria acontecer com o teto de 500, mas um lote grande
            # demais e rejeitado silenciosamente -- melhor falhar aqui.
            raise SystemExit(
                f"lote de {len(leituras)} passou de {MAX_PAYLOAD} bytes; "
                "reduza --lote"
            )
        mensagens.append(corpo)
    return mensagens


def principal():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tratores", type=int, default=3)
    p.add_argument("--horas", type=float, default=2.0,
                   help="duracao da jornada simulada, terminando agora")
    p.add_argument("--intervalo", type=int, default=5,
                   help="segundos entre fixes (case com PositionSampleSeconds)")
    p.add_argument("--velocidade", type=float, default=7.0, help="km/h de trabalho")
    p.add_argument("--implemento", type=float, default=12.0,
                   help="largura do implemento em metros; define o espacamento")
    p.add_argument("--comprimento", type=float, default=600.0,
                   help="comprimento da passada em metros")
    p.add_argument("--ruido", type=float, default=1.5,
                   help="desvio do ruido de GPS em metros")
    p.add_argument("--parada", type=float, default=25.0,
                   help="minutos de motor desligado no meio da jornada (0 desliga)")
    p.add_argument("--repasse", action="store_true", default=True,
                   help="repassa algumas faixas, para sobreposicao no heatmap")
    p.add_argument("--lat", type=float, default=-21.1700)
    p.add_argument("--lon", type=float, default=-47.8100)
    p.add_argument("--prefixo", default="trator")
    p.add_argument("--lote", type=int, default=MAX_ELEMENTOS)
    p.add_argument("--semente", type=int, default=42)
    p.add_argument("--saida", help="arquivo .jsonl com uma mensagem por linha")
    p.add_argument("--publicar", action="store_true",
                   help="publica no IoT Core (usa as credenciais do ambiente)")
    p.add_argument("--topico", default="devices/{device}/position",
                   help="tópico; {device} e substituido pelo id do trator")
    p.add_argument("--regiao")
    args = p.parse_args()

    if args.lote > MAX_ELEMENTOS:
        raise SystemExit(f"--lote nao pode passar de {MAX_ELEMENTOS}")
    if not args.saida and not args.publicar:
        raise SystemExit("informe --saida, --publicar, ou os dois")

    aleatorio = random.Random(args.semente)
    fim = datetime.now(timezone.utc).replace(microsecond=0)
    inicio = fim - timedelta(hours=args.horas)

    trabalho = []
    for indice in range(args.tratores):
        device = f"{args.prefixo}-{indice + 1:02d}"
        # Talhoes lado a lado, separados por uma faixa de manobra.
        base_lat, base_lon = desloca(
            args.lat, args.lon, 0, indice * (args.comprimento + 120)
        )
        fixes = rota(base_lat, base_lon, inicio, args, aleatorio)
        trabalho.append((device, fixes, em_lotes(fixes, device, args.lote)))

    total_fixes = sum(len(f) for _, f, _ in trabalho)
    total_msgs = sum(len(m) for _, _, m in trabalho)
    maior = max(len(m.encode()) for _, _, ms in trabalho for m in ms)
    print(f"{args.tratores} trator(es) · {args.horas} h · 1 fix/{args.intervalo}s")
    print(f"{total_fixes} fixes em {total_msgs} mensagens "
          f"(maior: {maior/1024:.1f} KB de {MAX_PAYLOAD/1024:.0f} KB)")
    print(f"janela: {inicio.isoformat()} .. {fim.isoformat()}")

    if args.saida:
        with open(args.saida, "w") as arquivo:
            for device, _, mensagens in trabalho:
                for corpo in mensagens:
                    arquivo.write(json.dumps({
                        "topic": args.topico.format(device=device),
                        "payload": json.loads(corpo),
                    }) + "\n")
        print(f"gravado em {args.saida}")

    if args.publicar:
        import boto3
        cliente = boto3.client("iot-data", region_name=args.regiao)
        enviadas = 0
        for device, _, mensagens in trabalho:
            topico = args.topico.format(device=device)
            for corpo in mensagens:
                cliente.publish(topic=topico, qos=1, payload=corpo.encode())
                enviadas += 1
                print(f"\r  publicadas {enviadas}/{total_msgs}", end="", flush=True)
        print(f"\npublicadas {enviadas} mensagens")


if __name__ == "__main__":
    sys.exit(principal())
