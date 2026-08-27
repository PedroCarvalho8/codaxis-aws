"""
Glue Job: le a tabela Iceberg de telemetria bruta, calcula rollups em tres
granularidades e grava no DynamoDB para o frontend consumir.

Cada bucket carrega min / max / avg / count. Media sozinha apaga o pico, que
num grafico de sensor costuma ser exatamente o que interessa.

A escrita e idempotente: o item e sobrescrito por (pk, sk), entao reprocessar
a mesma janela produz o mesmo resultado. Isso permite rodar com lookback
generoso para absorver dado que chegou atrasado.
"""

import math
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

ARGS = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "glue_database",
        "source_table",
        "dynamo_table",
        "lookback_hours",
        "ttl_days",
        "positions_table",
        "sample_seconds",
    ],
)

LOOKBACK_HOURS = int(ARGS["lookback_hours"])
TTL_DAYS = int(ARGS["ttl_days"])
DYNAMO_TABLE = ARGS["dynamo_table"]
SAMPLE_SECONDS = int(ARGS["sample_seconds"])

# Precisao do geohash -> (rotulo, tamanho aproximado da celula).
# p7 e a visao da fazenda; p9 e menor que a largura do implemento (6 a 12 m),
# que e a condicao para sobreposicao aparecer no mapa.
HEAT_GROSSA, HEAT_FINA = 7, 9

# Um ponto sem sucessor, ou com sucessor muito distante no tempo, nao pode
# arrastar horas de permanencia para a celula onde o trator estacionou.
DT_MAXIMO = SAMPLE_SECONDS * 3

# granularidade -> (expressao de truncamento, formato da chave, dias de TTL)
# TTL curto no minuto e longo no dia: o grafico de range largo nunca pede
# granularidade fina, entao guardar 1min por 45 dias so paga armazenamento.
GRANULARITIES = {
    "1min": ("minute", "yyyy-MM-dd'T'HH:mm", 7),
    "1h": ("hour", "yyyy-MM-dd'T'HH", TTL_DAYS),
    "1d": ("day", "yyyy-MM-dd", TTL_DAYS * 8),
}

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)

source = f"glue_catalog.{ARGS['glue_database']}.{ARGS['source_table']}"
cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

raw = (
    spark.read.format("iceberg")
    .load(source)
    .filter(F.col("event_time") >= F.lit(cutoff))
)

# Deduplicacao na origem: retry de um lote HTTP reenvia os mesmos registros.
# O exactly-once do Firehose garante entrega, nao ausencia de duplicata na
# origem -- (batch_id, seq) e o que resolve isso.
raw = raw.dropDuplicates(["device_id", "batch_id", "seq"])

if raw.head(1):

    def rollup(granularity: str):
        trunc_unit, key_format, ttl_days = GRANULARITIES[granularity]
        bucket = F.date_trunc(trunc_unit, F.col("event_time"))
        expires = int(
            (datetime.now(timezone.utc) + timedelta(days=ttl_days)).timestamp()
        )
        return (
            raw.withColumn("bucket_start", bucket)
            .groupBy("device_id", "metric", "bucket_start")
            .agg(
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
                F.avg("value").alias("avg_value"),
                F.count("*").alias("sample_count"),
                F.first("unit", ignorenulls=True).alias("unit"),
            )
            .select(
                F.concat_ws("#", F.lit("DEV"), "device_id", "metric").alias("pk"),
                F.concat_ws(
                    "#",
                    F.lit("AGG"),
                    F.lit(granularity),
                    F.date_format("bucket_start", key_format),
                ).alias("sk"),
                F.col("min_value"),
                F.col("max_value"),
                F.col("avg_value"),
                F.col("sample_count"),
                F.col("unit"),
                F.date_format("bucket_start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                    "bucket_start"
                ),
                F.lit(expires).alias("expires_at"),
            )
        )

    def write_partition(rows):
        """Grava uma particao Spark no DynamoDB.

        batch_writer faz o chunking de 25 itens e reenvia UnprocessedItems
        automaticamente, com backoff.
        """
        table = boto3.resource("dynamodb").Table(DYNAMO_TABLE)
        with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
            for row in rows:
                batch.put_item(
                    Item={
                        "pk": row["pk"],
                        "sk": row["sk"],
                        "bucket_start": row["bucket_start"],
                        "min": Decimal(str(round(row["min_value"], 6))),
                        "max": Decimal(str(round(row["max_value"], 6))),
                        "avg": Decimal(str(round(row["avg_value"], 6))),
                        "n": int(row["sample_count"]),
                        "unit": row["unit"] or "",
                        "expires_at": int(row["expires_at"]),
                    }
                )

    def write_catalog():
        """Grava um item por (device, metrica) sob a particao CATALOG.

        Sem isto, listar os dispositivos exigiria um Scan da tabela. Com o
        item de catalogo, a API resolve a listagem com uma Query em
        pk = "CATALOG", que nao cresce com o historico -- so com a
        quantidade de series distintas.

        O TTL e renovado a cada execucao: um device que parar de reportar
        some da listagem sozinho depois de TTL_DAYS, sem nenhuma limpeza.
        """
        expires = int(
            (datetime.now(timezone.utc) + timedelta(days=TTL_DAYS)).timestamp()
        )
        series = (
            raw.groupBy("device_id", "metric")
            .agg(
                F.max("event_time").alias("last_seen"),
                F.first("unit", ignorenulls=True).alias("unit"),
            )
            .collect()
        )
        table = boto3.resource("dynamodb").Table(DYNAMO_TABLE)
        with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
            for row in series:
                batch.put_item(
                    Item={
                        "pk": "CATALOG",
                        "sk": f"DEV#{row['device_id']}#{row['metric']}",
                        "device_id": row["device_id"],
                        "metric": row["metric"],
                        "unit": row["unit"] or "",
                        "last_seen": row["last_seen"].strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        "expires_at": expires,
                    }
                )
        print(f"[rollup] catalogo: {len(series)} series")

    for granularity in GRANULARITIES:
        df = rollup(granularity)
        total = df.count()
        print(f"[rollup] {granularity}: {total} buckets")
        if total:
            # Reparticiona para paralelizar a escrita sem estourar a
            # particao quente do DynamoDB.
            df.repartition(4).foreachPartition(write_partition)

    # Roda no driver: a cardinalidade e o numero de series, nao de linhas.
    write_catalog()
else:
    print("[rollup] nenhum registro na janela; nada a fazer")


# =============================================================================
# Posicao (GPS dos tratores) -> heatmap por celula de geohash
#
# Aqui a agregacao certa nao e temporal, e espacial: quanto tempo o trator
# permaneceu em cada pedaco de terreno. Geohash da isso com uma propriedade
# util de graca -- celulas vizinhas compartilham prefixo, entao begins_with no
# DynamoDB recorta uma regiao do mapa como between recorta um intervalo.
#
# O valor da celula e TEMPO, nao contagem de amostras. Contagem e enviesada
# pela taxa de reporte: perda de sinal ou reporte adaptativo mentem no mapa.
# =============================================================================

BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def geohash(lat, lon, precisao):
    """Codifica lat/lon em geohash. Sem dependencia: o Glue nao tem pacote extra.

    O prefixo e a propriedade que interessa aqui: celulas vizinhas no espaco
    compartilham prefixo, entao begins_with no DynamoDB recorta uma regiao do
    mapa do mesmo jeito que between recorta um intervalo de tempo.
    """
    lat_min, lat_max = -90.0, 90.0
    lon_min, lon_max = -180.0, 180.0
    codigo, bits, valor, par = [], 0, 0, True
    while len(codigo) < precisao:
        if par:
            meio = (lon_min + lon_max) / 2
            if lon >= meio:
                valor = valor * 2 + 1
                lon_min = meio
            else:
                valor = valor * 2
                lon_max = meio
        else:
            meio = (lat_min + lat_max) / 2
            if lat >= meio:
                valor = valor * 2 + 1
                lat_min = meio
            else:
                valor = valor * 2
                lat_max = meio
        par = not par
        bits += 1
        if bits == 5:
            codigo.append(BASE32[valor])
            bits, valor = 0, 0
    return "".join(codigo)


def centro(codigo):
    """Devolve (lat, lon) do centro da celula e o tamanho dela em graus."""
    lat_min, lat_max = -90.0, 90.0
    lon_min, lon_max = -180.0, 180.0
    par = True
    for caractere in codigo:
        valor = BASE32.index(caractere)
        for deslocamento in (16, 8, 4, 2, 1):
            bit = 1 if valor & deslocamento else 0
            if par:
                meio = (lon_min + lon_max) / 2
                if bit:
                    lon_min = meio
                else:
                    lon_max = meio
            else:
                meio = (lat_min + lat_max) / 2
                if bit:
                    lat_min = meio
                else:
                    lat_max = meio
            par = not par
    return ((lat_min + lat_max) / 2, (lon_min + lon_max) / 2,
            lat_max - lat_min, lon_max - lon_min)


def metros_entre(lat_a, lon_a, lat_b, lon_b):
    raio = 6371000.0
    d_lat = math.radians(lat_b - lat_a)
    d_lon = math.radians(lon_b - lon_a)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat_a))
        * math.cos(math.radians(lat_b))
        * math.sin(d_lon / 2) ** 2
    )
    return 2 * raio * math.asin(math.sqrt(a))


def trecho_em_celulas(lat, lon, prox_lat, prox_lon, precisao, passo_m):
    """Distribui um trecho entre as celulas que ele CRUZA, com peso.

    Atribuir o trecho inteiro a celula do ponto inicial subestima cobertura
    sempre que o deslocamento entre fixes for maior que a celula -- e ele e:
    a 7 km/h com fix a cada 5 s o trator anda ~9,7 m, contra 4,8 m de celula
    fina. O heatmap sairia pontilhado, com buraco em toda celula pulada.

    A amostragem do segmento e aproximada, mas o erro fica limitado ao passo,
    e o resultado deixa de depender da taxa de reporte do device.
    """
    if prox_lat is None or prox_lon is None:
        return [(geohash(lat, lon, precisao), 1.0)]
    distancia = metros_entre(lat, lon, prox_lat, prox_lon)
    passos = max(1, min(200, int(distancia / passo_m) + 1))
    contagem = {}
    for i in range(passos):
        fracao = (i + 0.5) / passos
        codigo = geohash(
            lat + (prox_lat - lat) * fracao,
            lon + (prox_lon - lon) * fracao,
            precisao,
        )
        contagem[codigo] = contagem.get(codigo, 0) + 1
    return [(codigo, n / passos) for codigo, n in contagem.items()]


TRECHO = ArrayType(
    StructType([
        StructField("gh", StringType()),
        StructField("peso", DoubleType()),
    ])
)
trecho_udf = F.udf(trecho_em_celulas, TRECHO)
centro_lat_udf = F.udf(lambda c: centro(c)[0], DoubleType())
centro_lon_udf = F.udf(lambda c: centro(c)[1], DoubleType())

# A janela de recomputacao do heatmap comeca no INICIO do dia mais antigo
# alcancado pelo lookback, e nao no lookback em si. Motivo: o item da celula
# acumula o dia inteiro e e sobrescrito por (pk, sk); recomputar so as ultimas
# horas e sobrescrever apagaria a permanencia acumulada antes. Recomputando o
# dia inteiro a escrita continua idempotente.
heat_inicio = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).replace(
    hour=0, minute=0, second=0, microsecond=0
)

posicoes = (
    spark.read.format("iceberg")
    .load(f"glue_catalog.{ARGS['glue_database']}.{ARGS['positions_table']}")
    .filter(F.col("event_time") >= F.lit(heat_inicio))
    .dropDuplicates(["device_id", "batch_id", "seq"])
)

if posicoes.head(1):
    ordem = Window.partitionBy("device_id").orderBy("event_time")

    passo = (
        posicoes.withColumn("prox_time", F.lead("event_time").over(ordem))
        .withColumn("prox_lat", F.lead("lat").over(ordem))
        .withColumn("prox_lon", F.lead("lon").over(ordem))
        # dt do ponto ate o proximo, limitado: sem o teto, um trator desligado
        # as 18h e religado as 6h doaria 12 h para a celula onde estacionou.
        .withColumn(
            "dt",
            F.least(
                F.greatest(
                    F.coalesce(
                        F.unix_timestamp("prox_time") - F.unix_timestamp("event_time"),
                        F.lit(SAMPLE_SECONDS),
                    ),
                    F.lit(0),
                ),
                F.lit(DT_MAXIMO),
            ),
        )
        # Haversine ate o proximo ponto. Distancia e dt saem do mesmo par, entao
        # velocidade = distancia / tempo fecha sem o device precisar informar.
        .withColumn(
            "dist_m",
            F.when(
                F.col("prox_lat").isNull(), F.lit(0.0)
            ).otherwise(
                F.lit(2 * 6371000.0)
                * F.asin(
                    F.sqrt(
                        F.pow(F.sin(F.radians(F.col("prox_lat") - F.col("lat")) / 2), 2)
                        + F.cos(F.radians(F.col("lat")))
                        * F.cos(F.radians(F.col("prox_lat")))
                        * F.pow(
                            F.sin(F.radians(F.col("prox_lon") - F.col("lon")) / 2), 2
                        )
                    )
                )
            ),
        )
        .withColumn("dia", F.date_format("event_time", "yyyy-MM-dd"))
        # Passo de amostragem ~ metade da celula: fino o bastante para nao
        # pular celula, grosso o bastante para nao explodir o numero de passos.
        .withColumn(
            "trechos_grossa",
            trecho_udf("lat", "lon", "prox_lat", "prox_lon",
                       F.lit(HEAT_GROSSA), F.lit(70.0)),
        )
        .withColumn(
            "trechos_fina",
            trecho_udf("lat", "lon", "prox_lat", "prox_lon",
                       F.lit(HEAT_FINA), F.lit(2.0)),
        )
    )

    def celulas(coluna_trechos, chaves_extras=()):
        """Explode os trechos e agrega por celula, ponderando dt e distancia."""
        return (
            passo.select(
                "device_id", "dia", "dt", "dist_m",
                F.explode(coluna_trechos).alias("trecho"),
            )
            .select(
                "device_id", "dia",
                F.col("trecho.gh").alias("gh"),
                (F.col("dt") * F.col("trecho.peso")).alias("dt"),
                (F.col("dist_m") * F.col("trecho.peso")).alias("dist_m"),
                F.col("trecho.peso").alias("peso"),
            )
            .groupBy("device_id", "dia", "gh", *chaves_extras)
            .agg(
                F.sum("dt").alias("secs"),
                F.sum("dist_m").alias("dist_m"),
                # n conta fixes equivalentes, nao linhas: um fix dividido
                # entre tres celulas contribui um terco para cada.
                F.sum("peso").alias("n"),
            )
            .withColumn("cell_lat", centro_lat_udf(F.col("gh")))
            .withColumn("cell_lon", centro_lon_udf(F.col("gh")))
        )

    def escreve_celulas(linhas, monta_chave, prefixo_sk, expira):
        table = boto3.resource("dynamodb").Table(DYNAMO_TABLE)
        with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
            for linha in linhas:
                batch.put_item(
                    Item={
                        "pk": monta_chave(linha),
                        "sk": f"{prefixo_sk}#{linha['gh']}",
                        "gh": linha["gh"],
                        "lat": Decimal(str(round(linha["cell_lat"], 7))),
                        "lon": Decimal(str(round(linha["cell_lon"], 7))),
                        "secs": int(round(float(linha["secs"]))),
                        "dist_m": Decimal(str(round(float(linha["dist_m"]), 2))),
                        "n": int(round(float(linha["n"]))),
                        "expires_at": expira,
                    }
                )

    expira_heat = int(
        (datetime.now(timezone.utc) + timedelta(days=TTL_DAYS * 8)).timestamp()
    )

    # Camada grossa: poucas celulas por dia, uma Query resolve a fazenda toda.
    grossa = celulas("trechos_grossa").collect()
    escreve_celulas(
        grossa,
        lambda l: f"HEAT#{l['device_id']}#{l['dia']}",
        f"GH{HEAT_GROSSA}",
        expira_heat,
    )

    # Camada fina: a particao e o PREFIXO da propria celula fina, entao as
    # duas camadas nao tem como divergir. O viewport do mapa diz quais pk
    # pedir, sem varrer o dia inteiro na resolucao de 5 m.
    fina = celulas("trechos_fina").withColumn(
        "gh_grossa", F.substring(F.col("gh"), 1, HEAT_GROSSA)
    )
    total_fina = fina.count()
    if total_fina:
        fina.repartition(4).foreachPartition(
            lambda linhas: escreve_celulas(
                linhas,
                lambda l: f"HEAT#{l['device_id']}#{l['dia']}#{l['gh_grossa']}",
                f"GH{HEAT_FINA}",
                expira_heat,
            )
        )

    # Ultima posicao conhecida: um item sobrescrito por device. E o que um mapa
    # ao vivo precisa, e sai de graca desta passagem.
    recentes = (
        posicoes.withColumn(
            "ordem", F.row_number().over(
                Window.partitionBy("device_id").orderBy(F.col("event_time").desc())
            )
        )
        .filter(F.col("ordem") == 1)
        .collect()
    )
    table = boto3.resource("dynamodb").Table(DYNAMO_TABLE)
    with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
        for linha in recentes:
            batch.put_item(
                Item={
                    "pk": f"DEV#{linha['device_id']}#position",
                    "sk": "LATEST",
                    "lat": Decimal(str(round(float(linha["lat"]), 7))),
                    "lon": Decimal(str(round(float(linha["lon"]), 7))),
                    "at": linha["event_time"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "expires_at": expira_heat,
                }
            )

    print(
        f"[heatmap] p{HEAT_GROSSA}: {len(grossa)} celulas | "
        f"p{HEAT_FINA}: {total_fina} celulas | "
        f"{len(recentes)} posicoes atuais"
    )
else:
    print("[heatmap] nenhuma posicao na janela; nada a fazer")

job.commit()
