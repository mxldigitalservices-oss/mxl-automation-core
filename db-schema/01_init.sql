-- ==================================================
-- ESQUEMA: estado_balanceador
-- PROYECTO: mxl-pro
-- PROPÓSITO: Persistencia del estado de las 7 llaves Gemini
--            para el microservicio gemini_key_balancer.py
-- AUDITADO POR: Claude - 2026-07-04 - APROBADO
-- ==================================================
--
-- Nota de seguridad: esta tabla NUNCA almacena el valor real
-- de ninguna API key. Solo guarda el alias (ej. 'GEMINI_KEY_1'),
-- que se resuelve contra las variables de entorno de Railway
-- en tiempo de ejecución.

CREATE TABLE IF NOT EXISTS estado_balanceador (
    key_alias            TEXT PRIMARY KEY,
    activa               BOOLEAN DEFAULT TRUE,      -- FALSE = desactivada permanentemente (ej. error 401/403)
    bloqueada_hasta      TIMESTAMPTZ,                -- cooldown temporal (rate limit / timeout)
    fallos_consecutivos  INT DEFAULT 0,
    ultimo_uso           TIMESTAMPTZ DEFAULT NOW(),
    tasa_exito           FLOAT DEFAULT 1.0,          -- media móvil ponderada de éxito (0.0 - 1.0)
    creado_en            TIMESTAMPTZ DEFAULT NOW(),
    actualizado_en       TIMESTAMPTZ DEFAULT NOW()
);

-- Índice para acelerar la consulta de selección de llave disponible
-- (usada en cada solicitud, con FOR UPDATE SKIP LOCKED)
CREATE INDEX IF NOT EXISTS idx_balanceador_disponibles
ON estado_balanceador (activa, bloqueada_hasta, ultimo_uso);

-- Trigger para mantener actualizado_en siempre correcto en cada UPDATE
CREATE OR REPLACE FUNCTION actualizar_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.actualizado_en = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_actualizar_timestamp ON estado_balanceador;
CREATE TRIGGER trg_actualizar_timestamp
    BEFORE UPDATE ON estado_balanceador
    FOR EACH ROW
    EXECUTE FUNCTION actualizar_timestamp();

-- Inicialización de las 7 llaves (idempotente: no duplica si ya existen)
INSERT INTO estado_balanceador (key_alias, activa, fallos_consecutivos, tasa_exito)
VALUES
    ('GEMINI_KEY_1', TRUE, 0, 1.0),
    ('GEMINI_KEY_2', TRUE, 0, 1.0),
    ('GEMINI_KEY_3', TRUE, 0, 1.0),
    ('GEMINI_KEY_4', TRUE, 0, 1.0),
    ('GEMINI_KEY_5', TRUE, 0, 1.0),
    ('GEMINI_KEY_6', TRUE, 0, 1.0),
    ('GEMINI_KEY_7', TRUE, 0, 1.0)
ON CONFLICT (key_alias) DO NOTHING;
