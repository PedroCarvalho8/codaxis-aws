"""
Glue Job: le a tabela Iceberg de telemetria bruta, calcula rollups em tres
granularidades e grava no DynamoDB para o frontend consumir.

Cada bucket carrega min / max / avg / count. Media sozinha apaga o pico, que
num grafico de sensor costuma ser exatamente o que interessa.

A escrita e idempotente: o item e sobrescrito por (pk, sk), entao reprocessar
a mesma janela produz o mesmo resultado. Isso permite rodar com lookback
generoso para absorver dado que chegou atrasado.
"""

import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

ARGS = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "glue_database",
        "source_table",
        "dynamo_table",
        "lookback_hours",
        "ttl_days",
    ],
)

LOOKBACK_HOURS = int(ARGS["lookback_hours"])
TTL_DAYS = int(ARGS["ttl_days"])
DYNAMO_TABLE = ARGS["dynamo_table"]

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

job.commit()
