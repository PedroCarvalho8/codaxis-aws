# Pipeline de ingestão IoT — IoT Core → Firehose → Iceberg → DynamoDB

Pilha CloudFormation declarada para ser vinculada a este repositório via
**Git sync** do CloudFormation: a cada push nesta branch o serviço reaplica a
pilha a partir de `template.yaml`, usando os parâmetros de
`deployment-file.yaml`.

```
template.yaml         # a pilha inteira
deployment-file.yaml  # parâmetros + tags lidos pelo Git sync
rollup_job.py         # script do Glue Job (a pilha publica no S3)
frontend/index.html   # dashboard (a pilha publica no S3 + CloudFront)
scripts/              # checagem de que os arquivos embutidos não divergiram
tests/                # testes dos handlers da API, sem subir nada na AWS
```

## Vincular no console

`CloudFormation → Create stack → With Git (sync from Git)` e preencha:

| Campo | Valor |
|---|---|
| Repository | este repositório |
| Branch | a branch que vai disparar o sync |
| Deployment file path | `deployment-file.yaml` |
| Stack name | `iot-telemetry` (ou o que preferir) |

Não é preciso inventar nome de bucket. Com `DataBucketName` vazio — que é o
default e o que o `deployment-file.yaml` faz — a pilha deriva
`<ProjectName>-data-<account-id>-<região>`, já globalmente único. O nome
efetivo sai no output `DataBucket`. Só preencha o parâmetro para reaproveitar
um bucket de nome específico; nesse caso o `AllowedPattern` valida o formato na
criação do change set (3 a 63 caracteres, só minúsculas, dígitos, ponto e
hífen), em vez de deixar o erro estourar no meio do create.

Se um deploy falhar no `CREATE`, a pilha para em `ROLLBACK_COMPLETE` — estado
que não aceita update. **Push de correção não conserta**: delete a pilha no
console primeiro, e o sync seguinte recria do zero. Falha na *validação* do
change set, como um parâmetro fora do `AllowedPattern`, não tem esse problema:
nenhum recurso é tocado e o próximo push já tenta de novo.

### Recursos que sobrevivem ao delete da pilha

`DataBucket` e `RollupTable` têm `DeletionPolicy: Retain` — o dado não some
junto com a pilha. O preço aparece ao recriar: os dois já existem, e a
validação prévia reprova o change set com

```
The following hook(s)/validation failed: [AWS::EarlyValidation::ResourceExistenceCheck]
```

Nenhum recurso é tocado nesse caso. Veja qual deles colidiu e limpe antes de
sincronizar de novo:

```bash
aws cloudformation describe-stack-events --stack-name <sua-pilha> --max-items 20

aws dynamodb delete-table --table-name iot-telemetry-rollups
aws s3 rb s3://<nome-do-bucket> --force
```

A tabela pode ficar para trás até de um `CREATE` que falhou lá no começo:
`RollupTable` não depende de nada, então o CloudFormation a cria em paralelo,
antes de recursos mais lentos falharem. Enquanto o pipeline não tiver dado
real, vale considerar tirar o `Retain` dos dois e recolocar antes de valer —
senão cada ciclo de recriar a pilha pede essa limpeza manual.

A role de IAM que o Git sync assume precisa poder criar os recursos da pilha,
inclusive as roles de IAM que ela declara (equivale ao `CAPABILITY_IAM` do
CLI). Depois de vinculado, o ciclo é `commit → push → sync`; alterar parâmetro
é editar `deployment-file.yaml` e dar push, não mexer no console.

## O script do Glue Job

Não há passo manual de upload. O recurso `RollupScript` publica
`scripts/rollup_job.py` no bucket durante o deploy, e o job aponta para ele via
`!GetAtt RollupScript.Uri` — o que também garante a ordem: o objeto existe
antes de o job ser criado, e portanto antes de o trigger horário disparar.

```yaml
  RollupScript:
    Type: AWS::CloudFormation::CustomResource
    Properties:
      ServiceToken: !GetAtt ScriptWriterFunction.Arn
      Bucket: !Ref DataBucket
      Key: scripts/rollup_job.py
      Content: |
        <conteúdo de rollup_job.py>
```

O conteúdo vai numa **propriedade** do custom resource, não no `ZipFile` da
função: Lambda inline tem teto de 4096 caracteres e o job passa disso. A função
é só um escritor genérico de ~20 linhas.

Efeito colateral útil: editar o script muda a propriedade, o CloudFormation
chama Update e o arquivo é reenviado no mesmo sync. O script versiona junto com
a pilha.

### As duas cópias

`rollup_job.py` continua existindo como arquivo — para lint, editor e execução
local — e seu conteúdo é repetido dentro do template. Se as duas divergirem, o
job roda código velho sem nenhum sinal. `scripts/check_embedded_script.py`
compara as duas e falha se saírem de sincronia; o workflow em
`.github/workflows/validate.yml` roda essa checagem, o `cfn-lint` e o
`py_compile` a cada push.

Ao editar o job, mude o arquivo **e** o bloco `Content` do template, mantendo o
recuo do bloco escalar. A checagem avisa se esquecer.

## Rodar sob demanda

Para disparar sem esperar o cron:

```bash
aws glue start-job-run --job-name iot-telemetry-rollups
```

### Deploy pelo CLI (alternativa ao Git sync)

```bash
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name iot-telemetry \
  --parameter-overrides \
      ProjectName=iot-telemetry \
      MqttTopicFilter='devices/+/telemetry' \
  --capabilities CAPABILITY_IAM

```

Para fixar um nome de bucket em vez do derivado, acrescente
`DataBucketName=<nome>` ao `--parameter-overrides`.

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

pk = CATALOG                      # uma linha por série, para a listagem
sk = DEV#<device_id>#<metric>
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

## API de leitura

HTTP API (API Gateway v2), duas rotas, cada uma com sua Lambda:

```
GET /devices
GET /devices/{device_id}/metrics/{metric}?from=<ISO>&to=<ISO>[&granularity=]
```

Nenhuma das duas faz `Scan` — a policy das funções só concede
`dynamodb:Query`, então isso não depende de disciplina de quem edita o código.
A série espelha a chave da tabela. A listagem depende do item de catálogo
descrito abaixo. A URL base sai no output `ApiEndpoint`.

### Catálogo de dispositivos

`pk = DEV#<device>#<metric>` responde "me dê a série desse device", mas não
responde "quais devices existem" — isso exigiria varrer a tabela. Por isso o
Glue Job mantém, a cada execução, um item por série sob uma partição fixa:

```
pk = CATALOG
sk = DEV#<device_id>#<metric>     + device_id, metric, unit, last_seen
```

`GET /devices` vira uma `Query` nessa partição, cujo custo acompanha o número
de séries distintas e não o tamanho do histórico. O TTL desses itens é
renovado a cada execução do job: um device que parar de reportar sai da
listagem sozinho depois de `HotStoreTtlDays`, sem rotina de limpeza.

```bash
API=$(aws cloudformation describe-stacks --stack-name <sua-pilha> \
  --query "Stacks[0].Outputs[?OutputKey=='ApiEndpoint'].OutputValue" --output text)

curl "$API/devices/sensor-teste/metrics/temperature\
?from=2026-08-26T00:00:00Z&to=2026-08-27T00:00:00Z"
```

```json
{
  "device_id": "sensor-teste", "metric": "temperature",
  "granularity": "1h", "count": 1, "truncated": false,
  "points": [
    { "t": "2026-08-27T02:00:00Z", "min": 23.0, "max": 24.4,
      "avg": 23.7, "n": 3, "unit": "C" }
  ]
}
```

`granularity` é opcional: sem ela, a API escolhe pela extensão do range (até
6 h → `1min`, até 30 d → `1h`, acima → `1d`), então o frontend só manda início
e fim. Passe explicitamente para forçar.

`Cache-Control` sai longo (`max-age=86400`) quando a janela pedida já fechou, e
curto (`max-age=30`) quando alcança o presente — bucket fechado é imutável.
Isso é o que faz um CloudFront na frente absorver a maior parte das leituras.

O `truncated` avisa quando a resposta bateu no teto de 5000 pontos; um range
tão largo deveria estar pedindo granularidade mais grossa.

### Limites e o que falta

**A API é aberta.** Não há autenticação: quem tiver a URL lê a telemetria de
qualquer device. Para um teste tudo bem; antes de valer, coloque um JWT
authorizer ou uma chave. O throttle do stage (10 req/s, burst 20) protege a
conta de um cliente em loop, não protege o dado.

`ApiCorsOrigin` está em `*` por padrão. Aponte para o domínio do frontend.

`tests/test_api_handler.py` exercita os dois handlers sem subir nada na AWS —
lê o código de dentro do template, troca o DynamoDB por um stub e confere,
entre outras coisas, que os prefixos de `sk` gerados batem com o formato que o
Glue Job grava. Roda no CI junto com o `cfn-lint`.

## Dashboard

`frontend/index.html` é uma página sem dependências — sem build, sem bundler,
sem CDN — publicada pela própria pilha. A URL sai no output `SiteUrl`.

O bucket do site **não é público**: quem lê é o CloudFront, via Origin Access
Control. Isso evita bucket aberto, dá HTTPS (o endpoint de website do S3 serve
só em HTTP) e é o que aproveita o `Cache-Control` longo que a API devolve para
janelas já fechadas.

A página descobre a URL da API em `config.json`, escrito pela pilha com o
endpoint real. É por isso que o `index.html` do repositório é idêntico ao
publicado — nenhum placeholder é substituído na publicação — o que permite a
checagem de divergência.

O gráfico mostra a faixa mín–máx atrás da linha da média. Média sozinha apaga
o pico, que num gráfico de sensor costuma ser exatamente o que interessa —
é a razão de o rollup guardar as quatro estatísticas.

`index.html` e `config.json` sobem com `Cache-Control: max-age=60`, então o
CloudFront revalida depois de um deploy em vez de servir versão velha. Não há
invalidação a rodar.

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
- **Tabela Iceberg de agregados**: hoje o rollup vai só para o DynamoDB. Se
  quiser histórico agregado além do TTL, adicione uma segunda tabela Iceberg
  e um `MERGE INTO` no job antes da escrita no Dynamo.
