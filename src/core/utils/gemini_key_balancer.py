# ==================================================
# NOMBRE DEL MÓDULO: gemini_key_balancer.py
# RUTA RECOMENDADA: src/core/utils/gemini_key_balancer.py
# PROPÓSITO: Microservicio HTTP (FastAPI) que gestiona rotación
#            inteligente de 7 llaves Gemini vía PostgreSQL.
# VERSIÓN: v1.4 (Resiliencia de conexión a PostgreSQL - auditoría Claude)
# DEPENDENCIAS: fastapi, uvicorn, asyncpg, python-dotenv, httpx
# VARIABLES DE ENTORNO REQUERIDAS:
#   - GEMINI_KEY_1 ... GEMINI_KEY_7
#   - POSTGRES_DATABASE_URL
#   - STATUS_API_KEY            (obligatoria, sin valor por defecto)
#   - INTERNAL_AUTH_SECRET      (obligatoria, sin valor por defecto - compartida con n8n)
#   - DB_POOL_SIZE              (opcional, default: 20)
#   - SLACK_WEBHOOK_URL         (opcional, para alertas)
#   - TELEGRAM_BOT_TOKEN        (opcional, para alertas)
#   - TELEGRAM_CHAT_ID          (opcional, para alertas)
#   - LOG_LEVEL                 (opcional, default: WARNING)
# ENDPOINTS:
#   - POST /solicitar-llave      -> requiere header X-Internal-Secret.
#                                    Devuelve key_alias + status + api_key_real
#                                    (solo si status OK o ALERT).
#   - POST /registrar-resultado  -> recibe {key_alias, exito: bool, tipo_error?: str}
#   - GET  /health                -> healthcheck para Railway
#   - GET  /status                -> diagnóstico (protegido por STATUS_API_KEY)
# POOL: asyncpg, min_size=5, max_size=20 (ajustable con DB_POOL_SIZE)
# RESILIENCIA: reintentos con backoff al iniciar el pool + reintento
#              automático de operaciones si la conexión se cae a mitad
#              de una consulta (ej. Railway reinicia el Postgres).
# AUDITADO POR: Claude - 2026-07-06 - v1.4 resiliencia asyncpg
# ==================================================

import os
import json
import asyncio
import secrets
import logging
import functools
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Request, Depends
from pydantic import BaseModel
import asyncpg
from dotenv import load_dotenv
import httpx

from src.core.api.crm_api import router as crm_router, crm_db

load_dotenv()

# ============================================================
# LOGGING (nunca expone llaves reales, solo alias y eventos)
# ============================================================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "WARNING"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("gemini_key_balancer")

# ============================================================
# CONFIGURACIÓN
# ============================================================
GEMINI_KEYS = [os.getenv(f"GEMINI_KEY_{i}") for i in range(1, 8)]
DATABASE_URL = os.getenv("POSTGRES_DATABASE_URL")
POOL_MIN = 5
POOL_MAX = int(os.getenv("DB_POOL_SIZE", 20))
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# CORRECCIÓN DE SEGURIDAD (auditoría Claude v1.2):
# Ya NO tiene valor por defecto. Si falta, el servicio no debe arrancar,
# igual que con las demás credenciales del sistema.
STATUS_API_KEY = os.getenv("STATUS_API_KEY")

# Secreto compartido entre n8n y este microservicio (auditoría Claude v1.3).
# Mismo criterio: SIN valor por defecto. Si falta, el servicio no arranca.
INTERNAL_AUTH_SECRET = os.getenv("INTERNAL_AUTH_SECRET")

if not all(GEMINI_KEYS):
    raise ValueError("Faltan una o más GEMINI_KEY_X en variables de entorno")
if not DATABASE_URL:
    raise ValueError("POSTGRES_DATABASE_URL no definida")
if not STATUS_API_KEY:
    raise ValueError("STATUS_API_KEY no definida (obligatoria para proteger /status)")
if not INTERNAL_AUTH_SECRET:
    raise ValueError("INTERNAL_AUTH_SECRET no definida (obligatoria para proteger /solicitar-llave)")

# Mapa alias -> valor real, construido una sola vez al arrancar.
# Evita reconstruir el índice ("GEMINI_KEY_3" -> posición 2) en cada petición
# y evita volver a leer os.environ en caliente en cada request.
MAPA_ALIAS_A_KEY = {
    f"GEMINI_KEY_{i}": GEMINI_KEYS[i - 1] for i in range(1, 8)
}

# ============================================================
# DECORADOR DE RESILIENCIA (auditoría Claude v1.4)
# ============================================================
# Si Railway reinicia la base de datos mientras una operación está en
# curso, asyncpg puede lanzar estas excepciones de conexión perdida.
# El decorador reintenta la operación UNA sola vez tras una breve espera,
# dando tiempo a que el pool reconecte con una conexión nueva.
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
            await asyncio.sleep(1)
            return await func(self, *args, **kwargs)
    return wrapper


# ============================================================
# MODELOS PYDANTIC
# ============================================================
class SolicitarLlaveResponse(BaseModel):
    key_alias: Optional[str]
    status: str  # "OK" | "ALERT" | "FATAL"
    detail: Optional[str] = None
    api_key_real: Optional[str] = None  # solo presente si status es OK o ALERT


class RegistrarResultadoRequest(BaseModel):
    key_alias: str
    exito: bool
    tipo_error: Optional[str] = None  # "rate_limit", "timeout", "auth", "desconocido"


# ============================================================
# BASE DE DATOS (asyncpg)
# ============================================================
class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def init_pool(self):
        """
        Crea el pool con reintentos y backoff exponencial (auditoría
        Claude v1.4). Si Railway está reiniciando el Postgres justo
        cuando este servicio arranca, esto evita que el proceso muera
        al primer intento fallido.
        """
        max_intentos = 5
        espera = 2  # segundos, se duplica en cada intento

        for intento in range(1, max_intentos + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    DATABASE_URL,
                    min_size=POOL_MIN,
                    max_size=POOL_MAX,
                    command_timeout=5,
                    # Recicla conexiones inactivas para evitar que se queden
                    # "colgadas" tras un reinicio silencioso del servidor.
                    max_inactive_connection_lifetime=180,
                    server_settings={"application_name": "gemini_key_balancer"},
                )
                await self._ensure_tables()
                logger.info("Pool de PostgreSQL inicializado correctamente")
                return
            except EXCEPCIONES_CONEXION_PERDIDA as e:
                logger.warning(
                    f"Intento {intento}/{max_intentos} de conexión a PostgreSQL "
                    f"falló ({type(e).__name__})"
                )
                if intento == max_intentos:
                    logger.critical(
                        "No se pudo conectar a PostgreSQL tras varios intentos. "
                        "El servicio no puede arrancar."
                    )
                    raise
                await asyncio.sleep(espera)
                espera *= 2

    async def _ensure_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS estado_balanceador (
                    key_alias TEXT PRIMARY KEY,
                    bloqueada_hasta TIMESTAMPTZ,
                    fallos_consecutivos INT DEFAULT 0,
                    ultimo_uso TIMESTAMPTZ DEFAULT NOW(),
                    tasa_exito FLOAT DEFAULT 1.0,
                    activa BOOLEAN DEFAULT TRUE
                );
            """)
            for i in range(1, 8):
                alias = f"GEMINI_KEY_{i}"
                await conn.execute("""
                    INSERT INTO estado_balanceador (key_alias, fallos_consecutivos, tasa_exito, activa)
                    VALUES ($1, 0, 1.0, TRUE)
                    ON CONFLICT (key_alias) DO NOTHING
                """, alias)

    # ------------------------------------------------------------
    # 1. SELECCIÓN CON BLOQUEO REAL (evita condición de carrera)
    # ------------------------------------------------------------
    @con_reintento_conexion
    async def get_best_key(self) -> dict:
        """
        Selecciona y reserva atómicamente la mejor llave disponible.
        FOR UPDATE SKIP LOCKED evita que dos procesos concurrentes
        (ej. workflows de Facebook y WhatsApp corriendo a la vez)
        tomen la misma llave.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("""
                    SELECT key_alias
                    FROM estado_balanceador
                    WHERE activa = TRUE
                      AND (bloqueada_hasta IS NULL OR bloqueada_hasta < NOW())
                    ORDER BY tasa_exito DESC, fallos_consecutivos ASC, ultimo_uso ASC NULLS FIRST
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """)

                if not row:
                    return {"encontrada": False}

                alias = row["key_alias"]
                await conn.execute("""
                    UPDATE estado_balanceador
                    SET ultimo_uso = NOW()
                    WHERE key_alias = $1
                """, alias)

                return {"encontrada": True, "key_alias": alias}

    # ------------------------------------------------------------
    # 2. MÁQUINA DE ESTADOS (reemplaza el bucle infinito original)
    # ------------------------------------------------------------
    async def obtener_llave_con_estados(self) -> dict:
        """
        OK     -> llave obtenida normalmente
        ALERT  -> las 7 activas están en cooldown; se usa fallback degradado
        FATAL  -> las 7 llaves fueron desactivadas permanentemente
        """
        MAX_INTENTOS = 3
        ESPERA_SEGUNDOS = 5

        for intento in range(1, MAX_INTENTOS + 1):
            resultado = await self.get_best_key()
            if resultado["encontrada"]:
                return {
                    "status": "OK",
                    "key_alias": resultado["key_alias"],
                    "detail": f"Llave asignada en intento {intento}"
                }

            hay_activas = await self._existe_llave_activa()
            if not hay_activas:
                await self._notificar_alerta("FATAL", None)
                return {
                    "status": "FATAL",
                    "key_alias": None,
                    "detail": "Las 7 llaves fueron desactivadas permanentemente. Requiere intervención humana."
                }

            if intento < MAX_INTENTOS:
                await asyncio.sleep(ESPERA_SEGUNDOS)

        fallback = await self._obtener_fallback_mas_proximo()
        await self._notificar_alerta("ALERT", fallback)
        return {
            "status": "ALERT",
            "key_alias": fallback,
            "detail": "Todas las llaves activas están en cooldown. Se usa la más próxima a liberarse."
        }

    @con_reintento_conexion
    async def _existe_llave_activa(self) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) as total FROM estado_balanceador WHERE activa = TRUE"
            )
            return row["total"] > 0

    @con_reintento_conexion
    async def _obtener_fallback_mas_proximo(self) -> Optional[str]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT key_alias
                FROM estado_balanceador
                WHERE activa = TRUE
                ORDER BY bloqueada_hasta ASC NULLS FIRST
                LIMIT 1
            """)
            return row["key_alias"] if row else None

    async def _notificar_alerta(self, nivel: str, key_alias: Optional[str]):
        """
        Notifica a Slack/Telegram. Nunca incluye el valor real de la API key,
        solo el alias (cumple protocolo de seguridad del proyecto).
        """
        payload = {
            "evento": "balanceador_degradado",
            "nivel": nivel,
            "key_alias_afectado": key_alias,
            "timestamp": datetime.now().isoformat()
        }

        if SLACK_WEBHOOK:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.post(SLACK_WEBHOOK, json={"text": json.dumps(payload, indent=2)})
            except Exception as e:
                logger.error(f"Error enviando a Slack: {type(e).__name__}")

        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.post(url, json={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": f"ALERTA {nivel}\n{json.dumps(payload, indent=2)}"
                    })
            except Exception as e:
                logger.error(f"Error enviando a Telegram: {type(e).__name__}")

        logger.warning(f"[ALERTA {nivel}] {json.dumps(payload)}")

    # ------------------------------------------------------------
    # 3. REGISTRO DE RESULTADOS (auth = desactivación permanente)
    # ------------------------------------------------------------
    @con_reintento_conexion
    async def registrar_resultado(self, key_alias: str, exito: bool, tipo_error: Optional[str] = None):
        async with self.pool.acquire() as conn:
            if exito:
                await conn.execute("""
                    UPDATE estado_balanceador
                    SET fallos_consecutivos = 0,
                        tasa_exito = (tasa_exito * 0.9) + (1 * 0.1),
                        bloqueada_hasta = NULL
                    WHERE key_alias = $1
                """, key_alias)
                return

            # Llave inválida/revocada: desactivación PERMANENTE (no cooldown)
            if tipo_error == "auth":
                await conn.execute("""
                    UPDATE estado_balanceador
                    SET activa = FALSE,
                        fallos_consecutivos = fallos_consecutivos + 1,
                        tasa_exito = 0.0
                    WHERE key_alias = $1
                """, key_alias)
                await self._notificar_alerta("FATAL_PARCIAL", key_alias)
                return

            # Fallo recuperable: cooldown temporal
            await conn.execute("""
                UPDATE estado_balanceador
                SET fallos_consecutivos = fallos_consecutivos + 1,
                    tasa_exito = (tasa_exito * 0.9) + (0 * 0.1)
                WHERE key_alias = $1
            """, key_alias)

            row = await conn.fetchrow(
                "SELECT fallos_consecutivos FROM estado_balanceador WHERE key_alias = $1",
                key_alias
            )

            if row["fallos_consecutivos"] >= 3:
                tiempo_bloqueo = 15 * 60 if tipo_error == "rate_limit" else 5 * 60
                await conn.execute("""
                    UPDATE estado_balanceador
                    SET bloqueada_hasta = NOW() + ($1 || ' seconds')::INTERVAL
                    WHERE key_alias = $2
                """, tiempo_bloqueo, key_alias)

    # ------------------------------------------------------------
    # 4. ESTADO DE TODAS LAS LLAVES (diagnóstico)
    # ------------------------------------------------------------
    @con_reintento_conexion
    async def get_status_all(self) -> List[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT key_alias,
                       activa,
                       bloqueada_hasta IS NOT NULL AND bloqueada_hasta > NOW() as bloqueada,
                       fallos_consecutivos,
                       tasa_exito,
                       ultimo_uso
                FROM estado_balanceador
                ORDER BY key_alias
            """)
            return [dict(row) for row in rows]


# ============================================================
# FASTAPI APP
# ============================================================
db = Database()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pool()
    await crm_db.init_pool()  # pool separado para clientes/proyectos/webhooks (crm_api.py)
    yield
    await db.pool.close()
    await crm_db.close()


app = FastAPI(title="Gemini Key Balancer", version="1.4", lifespan=lifespan)

# Rutas de CRM: /leads, /portal/*, /admin/* (ver src/core/api/crm_api.py)
app.include_router(crm_router)


# ============================================================
# DEPENDENCIA PARA PROTEGER /status
# ============================================================
async def verificar_api_key(request: Request):
    api_key = request.headers.get("X-API-Key")
    # secrets.compare_digest evita timing attacks (auditoría Claude v1.2)
    if not api_key or not secrets.compare_digest(api_key, STATUS_API_KEY):
        raise HTTPException(status_code=403, detail="API Key inválida o faltante")


# ============================================================
# DEPENDENCIA PARA PROTEGER /solicitar-llave (auditoría Claude v1.3)
# ============================================================
async def verificar_secreto_interno(request: Request):
    """
    Solo n8n (u otro servicio interno autorizado) debe poder pedir la
    API key real. Sin este header correcto, la petición se rechaza
    antes de tocar la base de datos.
    """
    secreto = request.headers.get("X-Internal-Secret")
    if not secreto or not secrets.compare_digest(secreto, INTERNAL_AUTH_SECRET):
        raise HTTPException(status_code=403, detail="Secreto interno inválido o faltante")


# ============================================================
# ENDPOINTS
# ============================================================
@app.post(
    "/solicitar-llave",
    response_model=SolicitarLlaveResponse,
    dependencies=[Depends(verificar_secreto_interno)]
)
async def solicitar_llave():
    try:
        resultado = await db.obtener_llave_con_estados()
        alias = resultado["key_alias"]

        # Solo se resuelve y se incluye la key real si hay un alias utilizable
        # (status OK o ALERT). En FATAL, alias es None y no hay nada que resolver.
        api_key_real = MAPA_ALIAS_A_KEY.get(alias) if alias else None

        return SolicitarLlaveResponse(
            key_alias=alias,
            status=resultado["status"],
            detail=resultado.get("detail"),
            api_key_real=api_key_real
        )
    except Exception as e:
        logger.error(f"Error en /solicitar-llave: {type(e).__name__}")
        # No se incluye el detalle interno de la excepción en la respuesta:
        # podría filtrar información sensible (rutas, nombres de variables, etc.)
        raise HTTPException(status_code=500, detail="Error interno al procesar la solicitud")


@app.post("/registrar-resultado")
async def registrar_resultado(req: RegistrarResultadoRequest):
    try:
        await db.registrar_resultado(req.key_alias, req.exito, req.tipo_error)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Error en /registrar-resultado: {type(e).__name__}")
        raise HTTPException(status_code=500, detail="Error interno al registrar el resultado")


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/status", dependencies=[Depends(verificar_api_key)])
async def status():
    """Diagnóstico del estado de las 7 llaves - protegido por STATUS_API_KEY."""
    return await db.get_status_all()


# ============================================================
# ENTRYPOINT (Railway)
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    logger.info(f"Arrancando Gemini Key Balancer en el puerto {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
