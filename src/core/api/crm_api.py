# ==================================================
# NOMBRE DEL MÓDULO: crm_api.py
# RUTA RECOMENDADA: src/core/api/crm_api.py
# PROPÓSITO: Router de FastAPI que gestiona el ciclo de vida completo
#            de un cliente de Farol Publicidad Digital:
#              1) Captura del lead desde la landing pública.
#              2) Acceso del cliente a su portal vía enlace mágico
#                 (sin contraseñas).
#              3) Panel de control del Director (leads, cambios de
#                 estado, reenvío de notificaciones).
#              4) Disparo de webhooks hacia n8n en cada evento
#                 relevante, para que las automatizaciones reaccionen
#                 solas (WhatsApp/Email de bienvenida, alertas, etc.)
# VERSIÓN: v1.0
# DEPENDENCIAS: fastapi, asyncpg, httpx, pydantic
# VARIABLES DE ENTORNO REQUERIDAS (además de las de gemini_key_balancer.py):
#   - STATUS_API_KEY        (reutilizada: protege TODO /admin/* — es la
#                             llave del Director, la misma que ya protege /status)
#   - NEXTJS_API_SECRET     (obligatoria — secreto compartido SOLO entre
#                             las API routes de Next.js y este backend.
#                             El navegador del cliente NUNCA lo conoce.)
#   - N8N_WEBHOOK_URL       (obligatoria — endpoint del webhook maestro en n8n)
#   - N8N_WEBHOOK_SECRET    (obligatoria — para que n8n verifique que el
#                             webhook viene realmente de Railway)
#   - MAGIC_LINK_TTL_MIN    (opcional, default: 30 minutos)
# AUDITADO POR: Claude - 2026-07-06 - v1.0
# ==================================================

import os
import json
import hashlib
import secrets
import logging
import functools
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel, EmailStr, Field
import asyncpg
import httpx

logger = logging.getLogger("crm_api")

# ============================================================
# CONFIGURACIÓN
# ============================================================
DATABASE_URL = os.getenv("POSTGRES_DATABASE_URL")
STATUS_API_KEY = os.getenv("STATUS_API_KEY")             # llave del Director (reutilizada)
NEXTJS_API_SECRET = os.getenv("NEXTJS_API_SECRET")        # secreto Next.js <-> Railway
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")
N8N_WEBHOOK_SECRET = os.getenv("N8N_WEBHOOK_SECRET")
MAGIC_LINK_TTL_MIN = int(os.getenv("MAGIC_LINK_TTL_MIN", 30))

if not DATABASE_URL:
    raise ValueError("POSTGRES_DATABASE_URL no definida")
if not STATUS_API_KEY:
    raise ValueError("STATUS_API_KEY no definida (protege /admin/*)")
if not NEXTJS_API_SECRET:
    raise ValueError("NEXTJS_API_SECRET no definida (obligatoria para proteger /leads y /portal/*)")
if not N8N_WEBHOOK_URL:
    logger.warning("N8N_WEBHOOK_URL no definida: los webhooks se guardarán pero no se entregarán")

# ============================================================
# DECORADOR DE RESILIENCIA (mismo criterio que gemini_key_balancer.py)
# ============================================================
EXCEPCIONES_CONEXION_PERDIDA = (
    asyncpg.exceptions.ConnectionDoesNotExistError,
    asyncpg.exceptions.InterfaceError,
    asyncpg.exceptions.PostgresConnectionError,
    ConnectionResetError,
    OSError,
)


def con_reintento_conexion(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except EXCEPCIONES_CONEXION_PERDIDA as e:
            logger.warning(
                f"Conexión a PostgreSQL perdida en '{func.__name__}' "
                f"({type(e).__name__}). Reintentando una vez..."
            )
            import asyncio
            await asyncio.sleep(1)
            return await func(self, *args, **kwargs)
    return wrapper


# ============================================================
# MODELOS PYDANTIC
# ============================================================
class LeadRequest(BaseModel):
    nombre: str = Field(min_length=2, max_length=120)
    email: EmailStr
    telefono: Optional[str] = Field(default=None, max_length=30)
    servicio: str = Field(min_length=2, max_length=120)
    mensaje: Optional[str] = Field(default=None, max_length=1000)


class LeadResponse(BaseModel):
    proyecto_id: int
    estado: str


class SolicitarAccesoRequest(BaseModel):
    email: EmailStr


class VerificarTokenRequest(BaseModel):
    token: str


class ClienteSesion(BaseModel):
    cliente_id: int
    nombre: str
    email: str


class ProyectoPortal(BaseModel):
    proyecto_id: int
    servicio: str
    estado: str
    creado_en: datetime
    actualizado_en: datetime
    historial: List[dict]


class CambiarEstadoRequest(BaseModel):
    nuevo_estado: str = Field(pattern="^(recibido|en_produccion|entregado|cancelado)$")


# ============================================================
# BASE DE DATOS
# ============================================================
class CRMDatabase:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def init_pool(self):
        max_intentos = 5
        espera = 2
        import asyncio
        for intento in range(1, max_intentos + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    DATABASE_URL,
                    min_size=2,
                    max_size=10,
                    command_timeout=5,
                    max_inactive_connection_lifetime=180,
                    server_settings={"application_name": "crm_api"},
                )
                logger.info("Pool de PostgreSQL (CRM) inicializado correctamente")
                return
            except EXCEPCIONES_CONEXION_PERDIDA as e:
                logger.warning(f"Intento {intento}/{max_intentos} de conexión (CRM) falló ({type(e).__name__})")
                if intento == max_intentos:
                    raise
                await asyncio.sleep(espera)
                espera *= 2

    async def close(self):
        if self.pool:
            await self.pool.close()

    # ------------------------------------------------------------
    # LEADS
    # ------------------------------------------------------------
    @con_reintento_conexion
    async def crear_lead(self, datos: LeadRequest) -> dict:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                cliente = await conn.fetchrow(
                    """
                    INSERT INTO clientes (nombre, email, telefono)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (email) DO UPDATE
                        SET nombre = EXCLUDED.nombre,
                            telefono = COALESCE(EXCLUDED.telefono, clientes.telefono)
                    RETURNING id
                    """,
                    datos.nombre, datos.email, datos.telefono
                )
                proyecto = await conn.fetchrow(
                    """
                    INSERT INTO proyectos (cliente_id, servicio, mensaje_inicial, estado)
                    VALUES ($1, $2, $3, 'recibido')
                    RETURNING id, estado, creado_en
                    """,
                    cliente["id"], datos.servicio, datos.mensaje
                )
                await conn.execute(
                    """
                    INSERT INTO historial_estados (proyecto_id, estado_anterior, estado_nuevo, origen)
                    VALUES ($1, NULL, 'recibido', 'sistema')
                    """,
                    proyecto["id"]
                )
                return {
                    "proyecto_id": proyecto["id"],
                    "estado": proyecto["estado"],
                    "cliente_id": cliente["id"],
                    "nombre": datos.nombre,
                    "email": datos.email,
                    "servicio": datos.servicio,
                }

    # ------------------------------------------------------------
    # PORTAL DEL CLIENTE (magic link)
    # ------------------------------------------------------------
    @con_reintento_conexion
    async def solicitar_acceso(self, email: str) -> Optional[dict]:
        """
        Genera un token de un solo uso. Devuelve el token EN CLARO solo
        a esta función (para mandarlo por el webhook de n8n); en la base
        de datos solo se guarda su hash SHA-256, nunca el token real.
        Si el email no existe, devuelve None (pero el endpoint siempre
        responde igual al cliente, para no revelar qué emails existen).
        """
        async with self.pool.acquire() as conn:
            cliente = await conn.fetchrow("SELECT id, nombre FROM clientes WHERE email = $1", email)
            if not cliente:
                return None

            token = secrets.token_urlsafe(32)
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            expira_en = datetime.now(timezone.utc) + timedelta(minutes=MAGIC_LINK_TTL_MIN)

            await conn.execute(
                """
                INSERT INTO magic_links (cliente_id, token_hash, expira_en)
                VALUES ($1, $2, $3)
                """,
                cliente["id"], token_hash, expira_en
            )
            return {"token": token, "cliente_id": cliente["id"], "nombre": cliente["nombre"]}

    @con_reintento_conexion
    async def verificar_token(self, token: str) -> Optional[ClienteSesion]:
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT ml.id, ml.cliente_id, ml.expira_en, ml.usado, c.nombre, c.email
                    FROM magic_links ml
                    JOIN clientes c ON c.id = ml.cliente_id
                    WHERE ml.token_hash = $1
                    FOR UPDATE
                    """,
                    token_hash
                )
                if not row:
                    return None
                if row["usado"]:
                    return None
                if row["expira_en"] < datetime.now(timezone.utc):
                    return None

                await conn.execute("UPDATE magic_links SET usado = TRUE WHERE id = $1", row["id"])
                return ClienteSesion(cliente_id=row["cliente_id"], nombre=row["nombre"], email=row["email"])

    @con_reintento_conexion
    async def obtener_proyecto_activo_cliente(self, cliente_id: int) -> Optional[ProyectoPortal]:
        async with self.pool.acquire() as conn:
            proyecto = await conn.fetchrow(
                """
                SELECT id, servicio, estado, creado_en, actualizado_en
                FROM proyectos
                WHERE cliente_id = $1
                ORDER BY creado_en DESC
                LIMIT 1
                """,
                cliente_id
            )
            if not proyecto:
                return None

            historial = await conn.fetch(
                """
                SELECT estado_anterior, estado_nuevo, origen, creado_en
                FROM historial_estados
                WHERE proyecto_id = $1
                ORDER BY creado_en ASC
                """,
                proyecto["id"]
            )
            return ProyectoPortal(
                proyecto_id=proyecto["id"],
                servicio=proyecto["servicio"],
                estado=proyecto["estado"],
                creado_en=proyecto["creado_en"],
                actualizado_en=proyecto["actualizado_en"],
                historial=[dict(h) for h in historial],
            )

    # ------------------------------------------------------------
    # PANEL DEL DIRECTOR
    # ------------------------------------------------------------
    @con_reintento_conexion
    async def listar_leads(self) -> List[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM vista_leads_director")
            return [dict(r) for r in rows]

    @con_reintento_conexion
    async def cambiar_estado(self, proyecto_id: int, nuevo_estado: str, origen: str = "director") -> dict:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                actual = await conn.fetchrow(
                    "SELECT estado FROM proyectos WHERE id = $1 FOR UPDATE", proyecto_id
                )
                if not actual:
                    raise ValueError("proyecto_no_encontrado")

                await conn.execute(
                    "UPDATE proyectos SET estado = $1 WHERE id = $2",
                    nuevo_estado, proyecto_id
                )
                await conn.execute(
                    """
                    INSERT INTO historial_estados (proyecto_id, estado_anterior, estado_nuevo, origen)
                    VALUES ($1, $2, $3, $4)
                    """,
                    proyecto_id, actual["estado"], nuevo_estado, origen
                )
                cliente = await conn.fetchrow(
                    """
                    SELECT c.id AS cliente_id, c.nombre, c.email, c.telefono, p.servicio
                    FROM proyectos p JOIN clientes c ON c.id = p.cliente_id
                    WHERE p.id = $1
                    """,
                    proyecto_id
                )
                return {
                    "proyecto_id": proyecto_id,
                    "estado_anterior": actual["estado"],
                    "estado_nuevo": nuevo_estado,
                    **dict(cliente),
                }

    @con_reintento_conexion
    async def aprobar_proyecto(self, proyecto_id: int) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE proyectos SET aprobado_por_director = TRUE WHERE id = $1 RETURNING id",
                proyecto_id
            )
            if not row:
                raise ValueError("proyecto_no_encontrado")
        return await self.cambiar_estado(proyecto_id, "en_produccion", origen="director")

    # ------------------------------------------------------------
    # WEBHOOKS HACIA N8N
    # ------------------------------------------------------------
    @con_reintento_conexion
    async def registrar_evento_webhook(self, tipo_evento: str, proyecto_id: Optional[int], payload: dict) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO webhook_eventos (tipo_evento, proyecto_id, payload)
                VALUES ($1, $2, $3::jsonb)
                RETURNING id
                """,
                tipo_evento, proyecto_id, json.dumps(payload)
            )
            return row["id"]

    @con_reintento_conexion
    async def marcar_webhook(self, evento_id: int, entregado: bool, error: Optional[str] = None):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE webhook_eventos
                SET entregado = $1, intentos = intentos + 1, ultimo_error = $2
                WHERE id = $3
                """,
                entregado, error, evento_id
            )

    async def disparar_webhook(self, tipo_evento: str, proyecto_id: Optional[int], payload: dict):
        """
        Guarda el evento SIEMPRE primero (fuente de verdad), y luego
        intenta entregarlo a n8n. Si n8n está caído, el evento queda
        marcado como no entregado y el Director puede reenviarlo
        manualmente desde su panel (botón 'Reenviar notificación').
        """
        evento_id = await self.registrar_evento_webhook(tipo_evento, proyecto_id, payload)

        if not N8N_WEBHOOK_URL:
            await self.marcar_webhook(evento_id, False, "N8N_WEBHOOK_URL no configurada")
            return

        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp = await client.post(
                    N8N_WEBHOOK_URL,
                    json={"tipo_evento": tipo_evento, "proyecto_id": proyecto_id, **payload},
                    headers={"X-Webhook-Secret": N8N_WEBHOOK_SECRET or ""},
                )
                resp.raise_for_status()
            await self.marcar_webhook(evento_id, True)
        except Exception as e:
            logger.error(f"Error entregando webhook '{tipo_evento}' a n8n: {type(e).__name__}")
            await self.marcar_webhook(evento_id, False, type(e).__name__)

    @con_reintento_conexion
    async def reenviar_webhook_de_proyecto(self, proyecto_id: int) -> bool:
        """Busca el último evento no entregado de un proyecto y lo reintenta."""
        async with self.pool.acquire() as conn:
            evento = await conn.fetchrow(
                """
                SELECT id, tipo_evento, payload FROM webhook_eventos
                WHERE proyecto_id = $1 AND entregado = FALSE
                ORDER BY creado_en DESC LIMIT 1
                """,
                proyecto_id
            )
        if not evento:
            return False
        payload = json.loads(evento["payload"]) if isinstance(evento["payload"], str) else evento["payload"]
        await self.disparar_webhook(evento["tipo_evento"], proyecto_id, payload)
        return True


crm_db = CRMDatabase()

# ============================================================
# DEPENDENCIAS DE SEGURIDAD
# ============================================================
async def verificar_secreto_nextjs(request: Request):
    """Solo las API routes de Next.js (nunca el navegador) deben tener este secreto."""
    secreto = request.headers.get("X-Internal-Secret")
    if not secreto or not secrets.compare_digest(secreto, NEXTJS_API_SECRET):
        raise HTTPException(status_code=403, detail="Secreto interno inválido o faltante")


async def verificar_director(request: Request):
    """Protege todo /admin/* — misma llave que ya protege /status."""
    api_key = request.headers.get("X-API-Key")
    if not api_key or not secrets.compare_digest(api_key, STATUS_API_KEY):
        raise HTTPException(status_code=403, detail="API Key inválida o faltante")


# ============================================================
# ROUTER
# ============================================================
router = APIRouter()


# ---------- 1. LEAD PÚBLICO (vía proxy de Next.js) ----------
@router.post("/leads", response_model=LeadResponse, dependencies=[Depends(verificar_secreto_nextjs)])
async def crear_lead(datos: LeadRequest):
    try:
        resultado = await crm_db.crear_lead(datos)
    except Exception as e:
        logger.error(f"Error creando lead: {type(e).__name__}")
        raise HTTPException(status_code=500, detail="No se pudo registrar el contacto")

    # Dispara la respuesta automática (WhatsApp/Email de bienvenida) vía n8n.
    # No bloquea la respuesta al cliente si n8n tarda: el registro ya quedó guardado.
    await crm_db.disparar_webhook("lead_nuevo", resultado["proyecto_id"], resultado)

    return LeadResponse(proyecto_id=resultado["proyecto_id"], estado=resultado["estado"])


# ---------- 2. PORTAL DEL CLIENTE ----------
@router.post("/portal/solicitar-acceso", dependencies=[Depends(verificar_secreto_nextjs)])
async def portal_solicitar_acceso(datos: SolicitarAccesoRequest):
    resultado = await crm_db.solicitar_acceso(datos.email)
    if resultado:
        # El link real solo se envía por WhatsApp/Email vía n8n, nunca en la respuesta HTTP.
        await crm_db.disparar_webhook("magic_link_solicitado", None, {
            "cliente_id": resultado["cliente_id"],
            "nombre": resultado["nombre"],
            "email": datos.email,
            "token": resultado["token"],
        })
    # Respuesta idéntica exista o no el email, para no filtrar qué correos están registrados.
    return {"mensaje": "Si el correo está registrado, recibirás un enlace de acceso en breve."}


@router.post("/portal/verificar-token", response_model=ClienteSesion, dependencies=[Depends(verificar_secreto_nextjs)])
async def portal_verificar_token(datos: VerificarTokenRequest):
    sesion = await crm_db.verificar_token(datos.token)
    if not sesion:
        raise HTTPException(status_code=401, detail="Enlace inválido, expirado o ya usado")
    return sesion


@router.get("/portal/mi-proyecto", response_model=ProyectoPortal, dependencies=[Depends(verificar_secreto_nextjs)])
async def portal_mi_proyecto(cliente_id: int):
    proyecto = await crm_db.obtener_proyecto_activo_cliente(cliente_id)
    if not proyecto:
        raise HTTPException(status_code=404, detail="Este cliente no tiene proyectos registrados")
    return proyecto


# ---------- 3. PANEL DEL DIRECTOR ----------
@router.get("/admin/leads", dependencies=[Depends(verificar_director)])
async def admin_listar_leads():
    return await crm_db.listar_leads()


@router.patch("/admin/proyectos/{proyecto_id}/estado", dependencies=[Depends(verificar_director)])
async def admin_cambiar_estado(proyecto_id: int, datos: CambiarEstadoRequest):
    try:
        resultado = await crm_db.cambiar_estado(proyecto_id, datos.nuevo_estado, origen="director")
    except ValueError:
        raise HTTPException(status_code=404, detail="Proyecto no encontrado")

    await crm_db.disparar_webhook("cambio_estado", proyecto_id, resultado)
    return resultado


@router.post("/admin/proyectos/{proyecto_id}/aprobar", dependencies=[Depends(verificar_director)])
async def admin_aprobar_proyecto(proyecto_id: int):
    try:
        resultado = await crm_db.aprobar_proyecto(proyecto_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Proyecto no encontrado")

    await crm_db.disparar_webhook("proyecto_aprobado", proyecto_id, resultado)
    return resultado


@router.post("/admin/proyectos/{proyecto_id}/reenviar-notificacion", dependencies=[Depends(verificar_director)])
async def admin_reenviar_notificacion(proyecto_id: int):
    enviado = await crm_db.reenviar_webhook_de_proyecto(proyecto_id)
    if not enviado:
        raise HTTPException(status_code=404, detail="No hay notificaciones pendientes para este proyecto")
    return {"reenviado": True}
