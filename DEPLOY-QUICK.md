# Deploy Rápido — oasis-radar

## 1. Copiar conteúdo para Portainer

1. Abrir: `http://10.0.0.10:9000`
2. Ir para **Stacks** → **Create Stack**
3. Nomear como: `oasis-radar`
4. Colar conteúdo de `portainer-snippet.v2.yml` na aba **Editor**
5. Definir Env vars (pode deixar default ou sobrescrever):
   ```
   FLOWER_BASIC_AUTH=admin:change-me-antes-de-produção
   OASIS_RADAR_CELERY_BROKER_URL=redis://oasis-veredas-redis:6379/0
   ```

## 2. Deploy

Clicar em **Deploy the Stack** e aguardar (1-2 min).

## 3. Validar

### Flower
```bash
curl -u admin:change-me-antes-de-produção http://10.0.0.10:5555/
# Esperado: página do Flower (HTML) ou código 200
```

### Loki ready
```bash
curl -fsS http://10.0.0.10:3100/ready
# Esperado: "ready"
```

### Labels disponíveis
```bash
curl -s http://10.0.0.10:3100/loki/api/v1/labels | jq .
# Esperado: lista de labels (compose_service, container, oasis_container, etc.)
```

## 4. Próximas revisões

Depois de estável:
- ajustar `FLOWER_BASIC_AUTH` com credencial real
- considerar monitoramento da retenção de logs
- integrar alertas com Sentry/observabilidade externa se necessário

---

**Tempo total estimado:** 5-10 minutos (including validation)
