-- ==================================================
-- ESQUEMA: crm_farol (clientes, proyectos, seguimiento)
-- PROYECTO: mxl-pro / Farol Publicidad Digital
-- PROPÓSITO: Persistencia del ciclo de vida completo de un cliente:
--            desde que llena el formulario de la landing hasta que
--            su proyecto se marca como "Entregado".
-- AUDITADO POR: Claude - 2026-07-06 - v1.0
-- ==================================================
--
-- Nota de seguridad: esta tabla NUNCA guarda contraseñas de clientes.
-- El acceso es por "enlace mágico" (token de un solo uso con expiración),
-- así que no hay contraseñas que filtrar ni que el cliente olvide.

-- ------------------------------------------------------------
-- 1. CLIENTES
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clientes (
    id                  SERIAL PRIMARY KEY,
    nombre              TEXT NOT NULL,
    email               TEXT NOT NULL UNIQUE,
    telefono            TEXT,                      -- para WhatsApp, formato E.164 ej. +18091234567
    creado_en           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clientes_email ON clientes (email);

-- ------------------------------------------------------------
-- 2. PROYECTOS (un cliente puede tener varios a lo largo del tiempo)
-- ------------------------------------------------------------
-- Estado válido: 'recibido' | 'en_produccion' | 'entregado' | 'cancelado'
CREATE TABLE IF NOT EXISTS proyectos (
    id                  SERIAL PRIMARY KEY,
    cliente_id          INT NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    servicio            TEXT NOT NULL,             -- ej. "Gestión de redes sociales"
    mensaje_inicial      TEXT,                      -- lo que el cliente escribió en el formulario
    estado              TEXT NOT NULL DEFAULT 'recibido'
                            CHECK (estado IN ('recibido', 'en_produccion', 'entregado', 'cancelado')),
    aprobado_por_director BOOLEAN DEFAULT FALSE,     -- el Director debe aprobar antes de pasar a producción
    creado_en           TIMESTAMPTZ DEFAULT NOW(),
    actualizado_en      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proyectos_cliente ON proyectos (cliente_id);
CREATE INDEX IF NOT EXISTS idx_proyectos_estado ON proyectos (estado);

DROP TRIGGER IF EXISTS trg_proyectos_actualizado ON proyectos;
CREATE TRIGGER trg_proyectos_actualizado
    BEFORE UPDATE ON proyectos
    FOR EACH ROW
    EXECUTE FUNCTION actualizar_timestamp();  -- reutiliza la función creada en 01_init.sql

-- ------------------------------------------------------------
-- 3. HISTORIAL DE ESTADOS (auditoría: quién cambió qué y cuándo)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS historial_estados (
    id                  SERIAL PRIMARY KEY,
    proyecto_id         INT NOT NULL REFERENCES proyectos(id) ON DELETE CASCADE,
    estado_anterior     TEXT,
    estado_nuevo        TEXT NOT NULL,
    origen              TEXT NOT NULL DEFAULT 'director', -- 'director' | 'sistema' | 'cliente'
    creado_en           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historial_proyecto ON historial_estados (proyecto_id, creado_en DESC);

-- ------------------------------------------------------------
-- 4. ENLACES MÁGICOS (acceso del cliente sin contraseña)
-- ------------------------------------------------------------
-- El token real NUNCA se guarda en texto plano: se guarda su hash SHA-256.
-- Así, aunque alguien lea la base de datos, no puede reconstruir un enlace válido.
CREATE TABLE IF NOT EXISTS magic_links (
    id                  SERIAL PRIMARY KEY,
    cliente_id          INT NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
    token_hash          TEXT NOT NULL UNIQUE,
    expira_en           TIMESTAMPTZ NOT NULL,
    usado               BOOLEAN DEFAULT FALSE,
    creado_en           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_magic_links_hash ON magic_links (token_hash);

-- Limpieza: los enlaces expirados no deben acumularse indefinidamente.
-- (Se invoca periódicamente desde el propio microservicio, no requiere cron externo.)

-- ------------------------------------------------------------
-- 5. EVENTOS DE WEBHOOK (bitácora de lo que se notificó a n8n)
-- ------------------------------------------------------------
-- Permite reintentar manualmente si n8n estuvo caído, y ver en el
-- Dashboard del Director si una notificación falló.
CREATE TABLE IF NOT EXISTS webhook_eventos (
    id                  SERIAL PRIMARY KEY,
    tipo_evento         TEXT NOT NULL,             -- ej. 'lead_nuevo', 'cambio_estado'
    proyecto_id         INT REFERENCES proyectos(id) ON DELETE SET NULL,
    payload             JSONB NOT NULL,
    entregado           BOOLEAN DEFAULT FALSE,
    intentos            INT DEFAULT 0,
    ultimo_error        TEXT,
    creado_en           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_pendientes ON webhook_eventos (entregado, creado_en);

-- ------------------------------------------------------------
-- VISTA DE CONVENIENCIA: para el Dashboard del Director
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vista_leads_director AS
SELECT
    p.id              AS proyecto_id,
    c.id              AS cliente_id,
    c.nombre,
    c.email,
    c.telefono,
    p.servicio,
    p.estado,
    p.aprobado_por_director,
    p.creado_en,
    p.actualizado_en
FROM proyectos p
JOIN clientes c ON c.id = p.cliente_id
ORDER BY p.creado_en DESC;
