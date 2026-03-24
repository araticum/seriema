# QUICK START — Deploy oasis-radar

## ⚡ 2 Minutos

### 1. Abra Portainer
```
http://10.0.0.10:9000
```

### 2. Stacks → Add Stack

- **Stack name:** `oasis-radar`
- **Compose:** copie `docker-compose.radar.yml` (completo)
- **Env vars:**
  ```
  OASIS_RADAR_CELERY_BROKER_URL=redis://oasis-veredas-redis:6379/0
  FLOWER_BASIC_AUTH=admin:<senha-url-safe-32chars>
  ```

### 3. Deploy (botão verde)

### 4. Aguarde ~30s, depois valide:
```bash
bash /home/node/.openclaw/workspace/OASIS/observability/validate-deploy.sh
```

---

## Acessar Serviços

| Serviço | URL | User/Pass |
|---------|-----|-----------|
| **Flower** | http://10.0.0.10:5555 | admin / senha definida no Portainer |
| **Loki** (API) | http://127.0.0.1:3100 | — |

---

## Se der erro?

**Containers não sobem:**
- Confirmar que rede `oasis_oasis-net` existe
- Confirmar que stack `oasis` está running

**Flower sem workers:**
- Confirmar que Redis (`oasis-veredas-redis`) está up
- Verificar broker URL: `redis://oasis-veredas-redis:6379/0`

**Loki sem logs:**
- Aguardar 5-10 minutos (Promtail precisa capturar logs)
- Verificar Promtail logs: `docker logs oasis-radar-promtail`

---

## Docs Completos

- `DEPLOY-MANUAL.md` — Instruções detalhadas
- `STATUS-DEPLOY.md` — Sumário técnico
- `validate-deploy.sh` — Script de validação
