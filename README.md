# Pipeline de ingestão IoT — IoT Core → Firehose → Iceberg → DynamoDB

Pilha CloudFormation declarada para ser vinculada a este repositório via
**Git sync** do CloudFormation: a cada push nesta branch o serviço reaplica a
pilha a partir de `template.yaml`, usando os parâmetros de
`deployment-file.yaml`.

```
template.yaml            # GERADO — não edite; é o que o Git sync aplica
deployment-file.yaml     # parâmetros + tags lidos pelo Git sync
Makefile                 # make build | make check

infra/                   # fontes do template
  header.yaml            #   cabeçalho e Description
  parameters.yaml        #   corpo de Parameters
  conditions.yaml        #   corpo de Conditions
  outputs.yaml           #   corpo de Outputs
  resources/             #   um arquivo por domínio, concatenados em ordem
    10-armazenamento.yaml
    20-catalogo-iceberg.yaml
    25-posicoes-iceberg.yaml
    30-firehose.yaml
    35-posicoes-firehose.yaml
    40-iot-core.yaml
    45-posicoes-iot.yaml
    50-dynamodb.yaml
    60-glue-job.yaml
    70-api.yaml
    80-frontend.yaml

glue/rollup_job.py       # script do Glue Job
lambdas/                 # handlers, um arquivo por função
  firehose_transform.py
  firehose_position_transform.py
  s3_writer.py
  api_query.py
  api_catalog.py
  api_heatmap.py
frontend/index.html      # dashboard

build/assemble.py        # monta template.yaml a partir do que está acima
tools/simula_posicoes.py # gera e publica massa de posição para testar
tests/                   # testes dos handlers, sem subir nada na AWS
```

### Por que o template é gerado

O Git sync aplica **um** arquivo de template, exatamente como está no
repositório. Ele não roda `aws cloudformation package`, então não existe
`!Include` nativo, e nested stack exigiria subir o template filho para o S3
antes — o mesmo problema do ovo e da galinha do script do Glue.

O código que precisa viver dentro do template (handlers inline das Lambdas, o
script do job, a página) somava 32 KB dos 75 KB do arquivo. `build/assemble.py`
resolve isso concatenando os fragmentos e expandindo marcadores:

```yaml
      Code:
        ZipFile: |
          {{ include: lambdas/api_query.py }}
```

O conteúdo entra recuado até a coluna do marcador. Assim cada handler é um
`.py` de verdade — lintável, testável, com syntax highlighting — e o
`frontend/index.html` abre no navegador.

A concatenação é textual, não uma mesclagem de objetos YAML, porque
round-trip por parser apagaria os comentários — e aqui eles carregam o porquê
de várias decisões.

### O ciclo

```bash
make build     # regenera template.yaml depois de mexer nas fontes
make check     # não altera nada; é o que o CI roda
```

`make check` falha se o `template.yaml` commitado estiver atrasado em relação
às fontes. Isso importa: como o Git sync lê só o template, uma fonte editada
sem rebuild passaria despercebida e o deploy usaria a versão antiga.

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
        {{ include: glue/rollup_job.py }}
```

O conteúdo vai numa **propriedade** do custom resource, não no `ZipFile` da
função: Lambda inline tem teto de 4096 caracteres e o job passa disso. A função
(`lambdas/s3_writer.py`) é só um escritor genérico de ~20 linhas, reaproveitado
para publicar também o `index.html` e o `config.json` do dashboard.

Efeito colateral útil: editar o script muda a propriedade, o CloudFormation
chama Update e o arquivo é reenviado no mesmo sync. O script versiona junto com
a pilha.

O script vive em `glue/rollup_job.py` e entra no template pelo mesmo
mecanismo de include das Lambdas — edite o arquivo e rode `make build`.

## Massa de teste

O cliente de teste do console publica uma mensagem por vez, e a 5 s duas horas
de trabalho por trator já são 1440 fixes. `tools/simula_posicoes.py` monta a
rota, quebra nos limites do IoT Core (500 elementos por `PutRecordBatch`,
128 KB de payload MQTT) e publica:

```bash
# um .json por mensagem, com só o payload — pronto para colar no cliente de
# teste do console; gera junto um publicar.sh que só precisa da AWS CLI
python3 tools/simula_posicoes.py --lote 250 --saida-dir /tmp/lotes

# gerar e publicar via boto3
python3 tools/simula_posicoes.py --publicar --regiao us-east-1
```

`--saida` (um `.jsonl` com `{topic, payload}` por linha) serve a um publicador
próprio, **não** ao console: colar uma dessas linhas publicaria o envelope, e
a regra faz `SELECT VALUE positions` na raiz — não acharia nada e não daria
erro visível. Para colar à mão, use `--saida-dir`.

A rota não é ruído: é serpentina de passadas paralelas com manobra de
cabeceira (onde a permanência se concentra), uma faixa repassada para a
sobreposição aparecer, e uma parada de motor desligado — que existe para
exercitar o teto de `dt`. Sem o teto, essa parada doaria a duração inteira
para a célula onde o trator ficou.

O padrão são 3 tratores, 2 h, 5 s: 3420 fixes em 9 mensagens.

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

## Posição dos tratores

Caminho de ingestão **paralelo**, em tópico e stream próprios:

```
devices/+/position → regra IoT → Firehose → positions_raw (Iceberg)
```

```json
{"positions": [
  {"device_id":"trator-01","event_time":"2026-08-27T14:00:00Z",
   "lat":-23.5505,"lon":-46.6333,"speed":8.2,"heading":137,
   "batch_id":"b-1","seq":0}
]}
```

`speed`, `heading` e `altitude` são opcionais — o job deriva velocidade de
distância/tempo, então o firmware não precisa calcular nada.

Separado de `telemetry_raw` porque o formato é outro. Uma leitura escalar é
`(metric, value)`; uma posição é um par `(lat, lon)` que só significa alguma
coisa junto. Modelar posição como duas métricas — `latitude` e `longitude` —
ingere sem erro e **destrói o pareamento**: sem ele não há distância,
velocidade nem ponto no mapa, porque trajetória é sequência ordenada e
`groupBy` + `min/max/avg` é operação sobre conjunto.

A Lambda de transformação rejeita coordenada fora do globo. `(0, 0)` é o fix
de GPS inválido clássico, e deixá-lo entrar poria uma célula quente no golfo
da Guiné.

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

pk = HEAT#<device>#<dia>                 # heatmap, visão da fazenda
sk = GH7#<geohash7>                      #   célula ~153 m

pk = HEAT#<device>#<dia>#<geohash7>      # heatmap, detalhe do talhão
sk = GH9#<geohash9>                      #   célula ~4,8 m

pk = DEV#<device>#position               # última posição conhecida
sk = LATEST
```

### Por que geohash

Geohash é **prefixo lexicográfico**: células vizinhas no espaço compartilham
prefixo. É o mesmo truque do ISO-8601 aplicado ao mapa — `begins_with` no `sk`
recorta uma região como `between` recorta um intervalo de tempo.

E a célula grossa é a **partição** da fina. Zoom out lê a camada de 153 m
(≈43 células por 100 ha, uma `Query`); zoom in, as células de 153 m visíveis
são as chaves de partição da camada de 4,8 m, então o cliente pede só o que
está na tela. Sem `Scan`, sem índice secundário.

A precisão 9 não é arbitrária: implemento típico tem 6 a 12 m de largura, e a
célula precisa ser menor que o implemento, senão sobreposição — que é o que o
heatmap deveria denunciar — nunca aparece.

### O trecho é distribuído entre as células que cruza

Atribuir o trecho inteiro à célula do ponto inicial subestima a cobertura
sempre que o deslocamento entre fixes for maior que a célula — e ele é: a
7 km/h com fix a cada 5 s o trator anda **9,7 m**, contra 4,8 m de célula
fina. O mapa sairia pontilhado, com buraco em toda célula pulada.

O job amostra cada segmento a meia-célula e reparte `dt` e distância entre as
células cruzadas, com peso. O resultado deixa de depender da taxa de reporte
do device.

**Limitação conhecida:** o que é pintado é o caminho da antena, não a faixa do
implemento. Para 11 km percorridos isso dá ~4,7 ha (uma linha de células de
4,8 m), enquanto um implemento de 12 m cobriria ~13 ha. Para cobertura de
verdade — e para sobreposição virar um número confiável — o segmento
precisaria ser espalhado também na perpendicular, pela largura do implemento.

### O valor da célula é tempo, não contagem

Contar amostras parece natural e é enganoso: a contagem é enviesada pela taxa
de reporte, então perda de sinal ou reporte adaptativo mentem no mapa. A
célula guarda **segundos de permanência**, somando o `dt` entre fixes
consecutivos, com teto em `3 × PositionSampleSeconds`. Sem o teto, um trator
desligado às 18h e religado às 6h doaria doze horas para a célula onde
estacionou.

Distância sai do mesmo par de pontos (haversine), então velocidade =
distância / tempo fecha sem o device informar nada.

### Idempotência

O item da célula acumula o dia inteiro e é sobrescrito por `(pk, sk)`. Por
isso a etapa espacial recomputa **o dia inteiro** alcançado pelo lookback, e
não só as últimas horas: recomputar parcialmente e sobrescrever apagaria a
permanência acumulada antes. Custa ler ~1 dia de posições a mais por execução
— nada, no volume de 5 s por trator.

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
GET /devices/{device_id}/heatmap?from=<AAAA-MM-DD>&to=<AAAA-MM-DD>[&cell=]
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

No heatmap, `cell` é o que troca de resolução: sem ele vêm as células de
153 m do período; com uma célula de 153 m, vem o detalhe de 4,8 m **dentro
dela**. A mesma célula reaparecendo em dias diferentes é somada, e `cellSize`
volta na resposta em graus, para o cliente desenhar o retângulo sem precisar
decodificar geohash. Teto de 31 dias por chamada.

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

`tests/test_api_handler.py` exercita os dois handlers sem subir nada na AWS.
Ele lê o código de dentro do `template.yaml` gerado, e não de `lambdas/*.py`,
de propósito: o que interessa testar é o artefato que a pilha vai implantar.
Troca o DynamoDB por um stub e confere, entre outras coisas, que os prefixos de
`sk` gerados batem com o formato que o Glue Job grava.

## Dashboard

`frontend/index.html` é uma página sem dependências — sem build, sem bundler,
sem CDN — publicada pela própria pilha. A URL sai no output `SiteUrl`.

O bucket do site **não é público**: quem lê é o CloudFront, via Origin Access
Control. Isso evita bucket aberto, dá HTTPS (o endpoint de website do S3 serve
só em HTTP) e é o que aproveita o `Cache-Control` longo que a API devolve para
janelas já fechadas.

A página descobre a URL da API em `config.json`, escrito pela pilha com o
endpoint real. É por isso que o `index.html` do repositório é publicado sem
nenhuma substituição: nada de placeholder, o arquivo que você abre no navegador
é o que vai para o ar.

O gráfico mostra a faixa mín–máx atrás da linha da média. Média sozinha apaga
o pico, que num gráfico de sensor costuma ser exatamente o que interessa —
é a razão de o rollup guardar as quatro estatísticas.

A aba **Mapa** desenha o heatmap de permanência. Clicar numa célula de 153 m
entra no detalhe de 4,8 m dela; o botão na trilha volta.

**Sem basemap, de propósito.** Um provedor de tiles seria dependência externa
e, para imagem de satélite, chave paga — decisão de conta, não técnica. Para
um talhão as próprias células já desenham o formato do campo. A projeção e o
hit-test estão isolados em `desenhaMapa` e `celulaEm`, então uma camada de
tiles entra por baixo depois sem mexer no resto.

As cores usam rampa sequencial de um único matiz, com quebras por **quantil**
e não lineares: a distribuição de permanência é muito assimétrica — quase toda
célula tem poucos segundos e umas poucas concentram o trabalho — e escala
linear pintaria o campo inteiro de uma cor só. No tema escuro a rampa inverte,
porque quem deve recuar em direção à superfície é o valor baixo.

O tile de área traz a resolução no rótulo de propósito: na célula de 153 m a
área superestima muito, porque a célula conta inteira mesmo quando o trator só
cruzou um canto. Só na célula fina isso vira cobertura de fato.

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

ALTER TABLE iot_telemetry_db.positions_raw
  ADD PARTITION FIELD day(event_time);

ALTER TABLE iot_telemetry_db.positions_raw
  ADD PARTITION FIELD bucket(16, device_id);
```

Na `positions_raw` isso pesa mais que na outra: a etapa espacial lê o dia
inteiro a cada execução (ver **Idempotência** acima), então sem partição por
dia ela varreria o histórico completo de hora em hora.

Sem isso a tabela funciona, mas todo scan lê o dataset inteiro — o que
degrada rápido conforme o histórico cresce.

## Custo — onde a conta muda

| Ponto | Ajuste |
|---|---|
| `FirehoseBufferSeconds` | 300 s gera arquivos maiores e menos compaction. Baixar para 60 s reduz latência e multiplica arquivos pequenos. |
| Mensageria IoT | Basic ingest elimina a cobrança do broker quando não há outros assinantes. |
| Lambda de transformação | Só carimba `ingested_at`. Se o device já mandar esse campo, remova o `ProcessingConfiguration` inteiro. |
| Glue Job | 2 workers G.1X por execução horária. Volume baixo pode virar execução de 4 em 4 h aumentando `RollupLookbackHours`. A etapa espacial roda no mesmo job — um segundo job dobraria o custo fixo de subida do Spark. |
| Posições | 5 s por trator × 10 h/dia = 7,2 mil linhas/trator/dia. Dez tratores dão 26 M/ano no Iceberg, o que é pouco; o que cresce é o número de células p9 no DynamoDB. |
| TTL do DynamoDB | `1min` expira em 7 dias, `1h` no valor de `HotStoreTtlDays`, `1d` em 8× isso. |

## O que não está aqui

- **Provisionamento de devices**: certificados X.509, IoT Policy por device e
  fleet provisioning ficam numa pilha separada, com ciclo de vida próprio.
- **Basemap no mapa**: as células são renderizadas sem imagem por baixo.
  Tiles de satélite exigem provedor externo e, em geral, chave paga.
- **Heatmap de frota**: hoje a partição é por trator. Um agregado por fazenda
  seria uma partição `HEAT#FLEET#<dia>` escrita pelo mesmo job.
- **Tabela Iceberg de agregados**: hoje o rollup vai só para o DynamoDB. Se
  quiser histórico agregado além do TTL, adicione uma segunda tabela Iceberg
  e um `MERGE INTO` no job antes da escrita no Dynamo.
