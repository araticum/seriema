# oasis-radar — observabilidade mínima do Oásis

Pacote enxuto para deploy em **stack separada** via Docker Compose/Portainer com:
- Loki (armazenamento e busca de logs)
- Promtail (coleta de logs de containers Docker)
- Flower (visibilidade operacional dos workers Celery)

Contexto assumido nesta v2:
- **Sentry é externo**
- **Langfuse é externo/cloud**
- o pacote local cobre **causa raiz + correlação de logs + fila Celery**
- **não mexe no compose principal do Oásis**

## Objetivo

Cobrir dois casos principais com baixo peso operacional:
1. busca e correlação de logs dos containers do stack Oásis;
2. visão operacional dos workers/queues Celery via Flower.

Sem Grafana/Prometheus nesta fase para manter o pacote leve e reversível.

## Artefatos

- `docker-compose.observability.yml` — compose local versionado da stack `oasis-radar`
- `portainer-snippet.v2.yml` — snippet v2 pronto para colar no editor do Portainer
- `.env.example` — variáveis mínimas de parametrização
- `loki/config.yml`
- `promtail/config.yml`

## Decisões da v2

- nome operacional padronizado para **`oasis-radar`** nos containers, volumes e documentação;
- stack continua **separada** do stack principal;
- rede interna dedicada: **`oasis-radar-net`**;
- conexão ao stack principal só onde precisa: **Flower** entra também na rede externa `oasis_oasis-net` para falar com o Redis já existente;
- Loki continua com bind local `127.0.0.1:3100` para reduzir exposição;
- snippet do Portainer ficou **autocontido**, sem depender de arquivos locais de config no host.

## Topologia

- **Rede própria da stack:** `oasis-radar-net`
- **Rede externa já existente do Oásis:** `oasis_oasis-net`
- **Serviços locais:** Loki + Promtail + Flower
- **Dependência externa do stack principal:** Redis do Veredas/Celery (`oasis-veredas-redis:6379/0` por padrão)

## Como subir

### Opção A — Portainer com snippet v2 (recomendado)

Criar uma nova stack chamada **`oasis-radar`** e colar o conteúdo de:
- `portainer-snippet.v2.yml`

Variáveis mínimas sugeridas no Portainer:
- `FLOWER_BASIC_AUTH=admin:trocar-antes-de-produção`
- `OASIS_RADAR_CELERY_BROKER_URL=redis://oasis-veredas-redis:6379/0`

Pré-requisito importante:
- a rede externa `oasis_oasis-net` precisa existir (criada pelo compose principal do Oásis).

### Opção B — Compose versionado no repositório

Usar `docker-compose.observability.yml` quando o deploy for feito fora do editor do Portainer e os arquivos locais de config estiverem montados a partir deste diretório.

## Janela calma — fluxo recomendado de subida

1. **Confirmar pré-requisito de rede**
   - a rede `oasis_oasis-net` já existe no host/Portainer.
2. **Preparar variáveis**
   - definir `FLOWER_BASIC_AUTH` com credencial própria;
   - confirmar se o broker Redis continua em `oasis-veredas-redis:6379/0`.
3. **Subir só a stack `oasis-radar`**
   - não tocar no compose principal do Oásis;
   - preferir janela calma para validar ingestão de logs e visibilidade do Celery sem ruído de deploy maior.
4. **Validar saúde dos três serviços**
   - Loki pronto;
   - Promtail enviando logs;
   - Flower listando workers/tasks.
5. **Observar por alguns minutos**
   - verificar labels principais no Loki;
   - checar se aparecem logs de `veredas-backend`, workers Celery e do próprio `oasis-radar`.

## Validação pós-deploy

### 1) Flower

Abrir:
- `http://HOST:5555`

Validar:
- tela abre com autenticação básica;
- workers Celery aparecem online;
- filas/tarefas recentes carregam sem erro.

### 2) Loki ready

No host:

```bash
curl -fsS http://127.0.0.1:3100/ready
```

Esperado:
- resposta `ready`

### 3) Labels principais disponíveis

Consulta útil para inspecionar labels:

```bash
curl -G 'http://127.0.0.1:3100/loki/api/v1/labels'
```

Esperado entre as labels:
- `compose_project`
- `compose_service`
- `container`
- `oasis_container`
- `stream`

### 4) Query básica de logs do Oásis

Backend do Veredas:

```bash
curl -G 'http://127.0.0.1:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={compose_service="veredas-backend"}'
```

Workers Celery do Oásis (ajuste o label se necessário conforme os containers reais):

```bash
curl -G 'http://127.0.0.1:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={container=~".*worker.*|.*celery.*"}'
```

Logs do próprio radar:

```bash
curl -G 'http://127.0.0.1:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={container=~"oasis-radar-.*"}'
```

### 5) Conferir descoberta do Promtail

Esperado:
- containers do Oásis entram com labels de compose;
- containers one-off não são ingeridos;
- `stderr` e `stdout` aparecem separados via label `stream`.

## Labels principais geradas pelo Promtail

- `container`
- `oasis_container`
- `compose_project`
- `compose_service`
- `stream`

Isso permite correlacionar:
- serviço do compose;
- nome real do container;
- projeto Docker Compose;
- stderr vs stdout.

## Exposição e segurança

- **Loki:** exposto apenas em `127.0.0.1:3100`
- **Promtail:** sem publicação externa
- **Flower:** exposto em `5555`, protegido por `basic_auth`

Se quiser endurecer depois:
- trocar Flower para bind local;
- publicar via reverse proxy autenticado;
- restringir ainda mais a regex de coleta do Promtail.

## Riscos conhecidos

- dependência do Docker socket no Promtail (`/var/run/docker.sock`);
- se o nome do Redis mudar no stack principal, o Flower perde conectividade até ajustar `OASIS_RADAR_CELERY_BROKER_URL`;
- se a rede externa `oasis_oasis-net` não existir, o deploy falha;
- o filtro atual do Promtail privilegia containers do Oásis/infra correlata; se houver novo padrão de nome, pode precisar ampliar regex.

## Rollback

Como a stack é separada, o rollback é simples:
1. parar/remover a stack `oasis-radar`;
2. confirmar que o stack principal do Oásis segue intacto;
3. remover volumes do radar apenas se quiser descartar estado local (`flower.db`, posições do promtail e dados do Loki).

## Sugestão operacional

Fazer a primeira subida em janela calma, validar ingestão/labels/Flower, e só depois considerar qualquer evolução de UX (ex.: Grafana). Nesta fase o ganho principal é **correlação rápida de causa raiz** com baixo custo operacional.
