# Deploy Manual — Stack oasis-radar

## Status: Pronto para Deploy

Arquivos preparados:
- ✅ `docker-compose.radar.yml` — compose completo (baseado em `portainer-snippet.v2.yml`)
- ✅ `.env` — credenciais de Flower (senha base64 de 32 caracteres)
- ✅ Configurações Loki, Promtail, Flower validadas

## Próximos Passos — via Portainer UI

1. **Acesse Portainer:** http://10.0.0.10:9000
2. **Stack → Add Stack**
3. **Nome:** `oasis-radar`
4. **Copiar o conteúdo de `docker-compose.radar.yml`** na área de texto
5. **Environment variables section:**
   - OASIS_RADAR_CELERY_BROKER_URL: `redis://oasis-veredas-redis:6379/0`
   - FLOWER_BASIC_AUTH: `admin:<senha-url-safe-32chars>` (copiar do `.env` local ou gerar no Portainer)
6. **Deploy** (botão verde)

⚠️ **IMPORTANTE:** A rede do Oásis (`oasis_oasis-net`) é referenciada como **external** no compose. Portainer deve criar a referência automaticamente ao deploying a nova stack.

## Credenciais para Portainer

```env
OASIS_RADAR_CELERY_BROKER_URL=redis://oasis-veredas-redis:6379/0
FLOWER_BASIC_AUTH=admin:<senha-url-safe-32chars>
```

## Validação Pós-Deploy

**Opção rápida (script):**
```bash
bash /home/node/.openclaw/workspace/OASIS/observability/validate-deploy.sh
```

**Ou validar manualmente:**

### 1. Containers Running
```bash
docker ps | grep oasis-radar
```
Esperado: 3 containers — `oasis-radar-loki`, `oasis-radar-promtail`, `oasis-radar-flower`

### 2. Loki Ready
```bash
curl -fsS http://127.0.0.1:3100/ready
```
Resposta esperada: `ready`

### 3. Labels Disponíveis
```bash
curl -s 'http://127.0.0.1:3100/loki/api/v1/labels' | grep -o '"[^"]*"'
```
Deve conter: `compose_project`, `compose_service`, `container`, `oasis_container`, `stream`

### 4. Query de Logs do Backend
```bash
curl -G 'http://127.0.0.1:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={compose_service="veredas-backend"}' \
  --data-urlencode 'start=now-1h' \
  --data-urlencode 'end=now' | head -50
```

### 5. Flower Acessível
- **URL:** http://10.0.0.10:5555
- **Username:** `admin`
- **Password:** [copiar de `.env`]
- Verificar: workers online, tarefas recentes

## Sinais de Problema

| Problema | Diagnóstico |
|----------|-------------|
| Deploy falha com erro "network not found" | A rede `oasis_oasis-net` não existe. **Solução:** Rodar o stack principal do Oásis primeiro |
| Flower sobe mas sem workers | Redis indisponível ou broker URL incorreta. Verificar: `docker ps \| grep redis` no host principal e confirmar Redis rodando |
| Loki pronto mas sem logs | Promtail pode não ter acesso ao Docker socket. Verificar volumes de montagem: `/var/run/docker.sock` e `/var/lib/docker/containers` |
| Flower retorna 401 (auth) | Credencial básica incorreta. Verificar: `FLOWER_BASIC_AUTH` no `.env` |
| Promtail error "Cannot connect to Docker daemon" | Socket Docker não montado corretamente. Volume `/var/run/docker.sock` precisa estar acessível no container |

## Rollback

1. Portainer → Stacks → `oasis-radar` → Remove
2. Volumes (opcional, para limpeza total): remover `oasis-radar-loki-data`, `oasis-radar-promtail-positions`, `oasis-radar-flower-data`

---

**Preparado em:** 2026-03-24 10:58 GMT-3
**Próximo passo:** Fazer deploy via Portainer UI com os arquivos acima
