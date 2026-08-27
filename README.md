# Pipeline de ingestão IoT — IoT Core → Firehose → Iceberg → DynamoDB

Pilha CloudFormation declarada para ser vinculada a este repositório via
**Git sync** do CloudFormation: a cada push nesta branch o serviço reaplica a
pilha a partir de `template.yaml`, usando os parâmetros de
`deployment-file.yaml`.

```
template.yaml         # a pilha inteira
deployment-file.yaml  # parâmetros + tags lidos pelo Git sync
rollup_job.py         # script do Glue Job (vai para o S3, não para a pilha)
```

## Vincular no console

`CloudFormation → Create stack → With Git (sync from Git)` e preencha:

| Campo | Valor |
|---|---|
| Repository | este repositório |
| Branch | a branch que vai disparar o sync |
| Deployment file path | `deployment-file.yaml` |
| Stack name | `iot-telemetry` (ou o que preferir) |

Antes do primeiro sync, **edite `DataBucketName`** em `deployment-file.yaml` —
nome de bucket S3 é globalmente único e o parâmetro não tem default. O
placeholder é propositalmente inválido, e o template valida o formato
(`AllowedPattern`), então um nome fora das regras do S3 é barrado já na
validação do change set em vez de estourar no meio do create. Regras: 3 a 63
caracteres, só minúsculas, dígitos, ponto e hífen, começando e terminando com
letra ou dígito. `iot-telemetry-data-<account-id>-<região>` costuma resolver a
unicidade.

Se um deploy falhar no `CREATE`, a pilha para em `ROLLBACK_COMPLETE` — estado
que não aceita update. **Push de correção não conserta**: delete a pilha no
console primeiro, e o sync seguinte recria do zero.

A role de IAM que o Git sync assume precisa poder criar os recursos da pilha,
inclusive as roles de IAM que ela declara (equivale ao `CAPABILITY_IAM` do
CLI). Depois de vinculado, o ciclo é `commit → push → sync`; alterar parâmetro
é editar `deployment-file.yaml` e dar push, não mexer no console.

## Ordem de deploy

O bucket é criado pela própria pilha e o Glue Job aponta para um script que
ainda não existe no momento do `create-stack`. O CloudFormation não valida a
existência do script, então o deploy passa — só não deixe o job disparar antes
do upload:

```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name iot-telemetry \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucket'].OutputValue" --output text)

aws s3 cp rollup_job.py s3://$BUCKET/scripts/rollup_job.py
```

O `AWS::Glue::Trigger` está com `StartOnCreation: true` e roda aos 5 minutos de
cada hora — se o primeiro sync terminar perto do minuto :05, a primeira
execução falha por script ausente e a seguinte já pega o arquivo. Para não ver
essa falha, suba o script logo após o `CREATE_COMPLETE`, ou entre com
`StartOnCreation: false` e habilite o trigger depois.

Para testar imediatamente:

```bash
aws glue start-job-run --job-name iot-telemetry-rollups
```

### Deploy pelo CLI (alternativa ao Git sync)

```bash
BUCKET=meu-bucket-iot-telemetria   # precisa ser globalmente único

aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name iot-telemetry \
  --parameter-overrides \
      ProjectName=iot-telemetry \
      DataBucketName=$BUCKET \
      MqttTopicFilter='devices/+/telemetry' \
  --capabilities CAPABILITY_IAM

aws s3 cp rollup_job.py s3://$BUCKET/scripts/rollup_job.py
```

Não misture os dois caminhos na mesma pilha: com o sync ativo, um deploy manual
é sobrescrito no push seguinte.

## Contrato do payload

O device publica um lote por mensagem:

```json
{
  "readings": [
    { "device_id": "sensor-01", "event_time": "2026-08-26T14:00:00Z",
      "metric": "temperature", "value": 23.4, "unit": "C",
      "batch_id": "b-778", "seq": 0 },
    { "device_id": "sensor-01", "event_time": "2026-08-26T14:00:10Z",
      "metric": "temperature", "value": 23.6, "unit": "C",
      "batch_id": "b-778", "seq": 1 }
  ]
}
```

`SELECT VALUE readings` + `BatchMode: true` faz cada elemento do array virar
um registro independente no `PutRecordBatch`. Teto de 500 elementos por
mensagem e 128 KB de payload MQTT — se o lote for maior, o device precisa
quebrar em mais de uma publicação.

Cada leitura carrega o próprio `device_id` porque, depois do fan-out do
array, a informação do tópico não acompanha o registro.

`batch_id` + `seq` é o par que permite deduplicar retry: o job faz
`dropDuplicates` sobre eles antes de agregar.

## Basic ingest

Se ninguém mais assina o tópico além da regra, publique em
`$aws/rules/iot_telemetry_telemetry` em vez de `devices/<id>/telemetry`. O
payload vai direto para o Rules Engine, pulando o message broker, e a
cobrança de mensageria some. O output `IoTRuleName` da pilha traz o nome
exato da regra.

## Consulta do frontend

Chaves na tabela de rollups:

```
pk = DEV#<device_id>#<metric>
sk = AGG#<granularidade>#<início do bucket>
```

Últimas 24 h em granularidade horária:

```
Query
  pk = "DEV#sensor-01#temperature"
  sk BETWEEN "AGG#1h#2026-08-25T14" AND "AGG#1h#2026-08-26T14"
```

O ordenamento lexicográfico do ISO-8601 coincide com o cronológico, então o
`between` já devolve a série ordenada. A API escolhe a granularidade pela
extensão do range (até 6 h → `1min`, até 30 d → `1h`, acima → `1d`) e o
frontend só manda início e fim.

Bucket fechado é imutável — `Cache-Control` longo e CloudFront na frente da
API derrubam a maior parte das leituras num dashboard com vários usuários
olhando os mesmos devices.

## Partition spec do Iceberg

O `AWS::Glue::Table` com `OpenTableFormatInput` cria a tabela, mas não aplica
partition spec com transform. Rode uma vez no Athena, depois do deploy:

```sql
ALTER TABLE iot_telemetry_db.telemetry_raw
  ADD PARTITION FIELD day(event_time);

ALTER TABLE iot_telemetry_db.telemetry_raw
  ADD PARTITION FIELD bucket(16, device_id);
```

Sem isso a tabela funciona, mas todo scan lê o dataset inteiro — o que
degrada rápido conforme o histórico cresce.

## Custo — onde a conta muda

| Ponto | Ajuste |
|---|---|
| `FirehoseBufferSeconds` | 300 s gera arquivos maiores e menos compaction. Baixar para 60 s reduz latência e multiplica arquivos pequenos. |
| Mensageria IoT | Basic ingest elimina a cobrança do broker quando não há outros assinantes. |
| Lambda de transformação | Só carimba `ingested_at`. Se o device já mandar esse campo, remova o `ProcessingConfiguration` inteiro. |
| Glue Job | 2 workers G.1X por execução horária. Volume baixo pode virar execução de 4 em 4 h aumentando `RollupLookbackHours`. |
| TTL do DynamoDB | `1min` expira em 7 dias, `1h` no valor de `HotStoreTtlDays`, `1d` em 8× isso. |

## O que não está aqui

- **Provisionamento de devices**: certificados X.509, IoT Policy por device e
  fleet provisioning ficam numa pilha separada, com ciclo de vida próprio.
- **API de leitura**: API Gateway + Lambda sobre a tabela de rollups. Não
  exponha credencial AWS direto no frontend para ler DynamoDB.
- **Tabela Iceberg de agregados**: hoje o rollup vai só para o DynamoDB. Se
  quiser histórico agregado além do TTL, adicione uma segunda tabela Iceberg
  e um `MERGE INTO` no job antes da escrita no Dynamo.
- **Upload do `rollup_job.py`**: o Git sync só aplica a pilha; o script não
  chega ao S3 sozinho. Se quiser automatizar, um workflow de CI que faz
  `aws s3 cp` no mesmo push resolve.
