# mxl-automation-core

Sistema centralizado de gestión y automatización para **Farol Publicidad Digital**: balanceo inteligente de llaves Gemini + CRM de clientes/proyectos + webhooks hacia n8n.

## Estructura del Proyecto
- `src/core/utils/gemini_key_balancer.py`: App principal de FastAPI. Balanceo de las 7 llaves Gemini + monta el router de CRM.
- `src/core/api/crm_api.py`: Leads, portal del cliente (magic link) y panel del Director.
- `db-schema/01_init.sql`: Tabla de estado del balanceador de llaves.
- `db-schema/02_crm_schema.sql`: Tablas de clientes, proyectos, historial de estados, magic links y webhooks. **Ejecutar después de 01_init.sql** (usa una función definida ahí).
- `workflows/template_n8n.json`: Flujo original de balanceo de Gemini.
- `workflows/template_n8n_crm_eventos.json`: Flujo nuevo que recibe los eventos del CRM (lead nuevo, magic link, cambios de estado) y dispara las notificaciones automáticas.

## Arquitectura del ecosistema

```
Cliente (navegador)  →  Next.js (Vercel)  →  Railway (FastAPI)  →  Postgres
                                                     ↓
                                                    n8n  →  WhatsApp / Email
```

El navegador **nunca** habla directo con Railway. Next.js actúa siempre de intermediario
(sus API routes internas) y es el único que conoce `NEXTJS_API_SECRET`.

## Variables de entorno (Railway)

| Variable | Obligatoria | Descripción |
|---|---|---|
| `GEMINI_KEY_1` … `GEMINI_KEY_7` | Sí | Llaves de Gemini |
| `POSTGRES_DATABASE_URL` | Sí | Conexión a Postgres |
| `STATUS_API_KEY` | Sí | Llave del Director. Protege `/status` y todo `/admin/*` |
| `INTERNAL_AUTH_SECRET` | Sí | Secreto compartido con n8n para `/solicitar-llave` |
| `NEXTJS_API_SECRET` | Sí | Secreto compartido SOLO con las API routes de Next.js. Protege `/leads` y `/portal/*` |
| `N8N_WEBHOOK_URL` | Sí (para notificaciones) | URL del webhook `farol-webhook-eventos` en n8n |
| `N8N_WEBHOOK_SECRET` | Sí | Para que n8n verifique que el webhook viene de Railway |
| `MAGIC_LINK_TTL_MIN` | No (default 30) | Minutos de validez del enlace mágico del cliente |
| `DB_POOL_SIZE` | No (default 20) | Tamaño del pool del balanceador |

## Endpoints

### Públicos (vía proxy de Next.js — requieren `X-Internal-Secret: NEXTJS_API_SECRET`)
- `POST /leads` — Crea/actualiza el cliente y su proyecto. Dispara el webhook `lead_nuevo`.
- `POST /portal/solicitar-acceso` — El cliente pide su enlace mágico por email.
- `POST /portal/verificar-token` — Valida el token de un solo uso y devuelve la sesión del cliente.
- `GET /portal/mi-proyecto?cliente_id=` — Estado y el historial del proyecto del cliente.

### Panel del Director (requieren `X-API-Key: STATUS_API_KEY`)
- `GET /admin/leads` — Lista de todos los clientes/proyectos.
- `PATCH /admin/proyectos/{id}/estado` — Cambia el estado (`recibido`/`en_produccion`/`entregado`/`cancelado`).
- `POST /admin/proyectos/{id}/aprobar` — Aprueba el proyecto y lo pasa a producción.
- `POST /admin/proyectos/{id}/reenviar-notificacion` — Reintenta el último webhook no entregado de ese proyecto.
- `GET /status` — Diagnóstico del balanceador de llaves (ya existente).

### Internas (n8n ↔ balanceador)
- `POST /solicitar-llave`, `POST /registrar-resultado`, `GET /health` (ya existentes, sin cambios).

## Orden de despliegue

1. Ejecuta `db-schema/01_init.sql` y luego `db-schema/02_crm_schema.sql` en tu Postgres de Railway.
2. Configura todas las variables de entorno de la tabla de arriba.
3. Importa `workflows/template_n8n_crm_eventos.json` en n8n y reemplaza los nodos HTTP de WhatsApp/Email por tu proveedor real (el archivo trae notas indicando dónde).
4. Despliega el frontend en Vercel (ver el README del proyecto `farol-frontend`) apuntando a esta API de Railway.

## Protocolo de seguridad
- Las llaves de Gemini nunca se guardan en la base de datos, solo su alias.
- Los enlaces mágicos de acceso al portal se guardan como hash SHA-256, nunca en texto plano.
- Cada endpoint sensible está protegido por un secreto distinto según quién debe poder llamarlo (Next.js, n8n o el Director), siguiendo el principio de mínimo privilegio.
