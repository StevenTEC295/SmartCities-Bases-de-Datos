-- Funciones almacenadas. Solo leen datos (STABLE): no modifican ninguna tabla.
-- Idempotente: CREATE OR REPLACE. Se ejecuta despues de las tablas (orden alfabetico en el init),
-- por eso se desactiva la validacion del cuerpo al crear la funcion.
SET check_function_bodies = off;

-- Accidentes por nivel de severidad (1 a 4), con filtros opcionales de fechas (UTC) y de estado.
-- Parametros en NULL = sin filtro.
CREATE OR REPLACE FUNCTION accidentes_por_severidad(
    p_ini    TIMESTAMP DEFAULT NULL,
    p_fin    TIMESTAMP DEFAULT NULL,
    p_estado TEXT      DEFAULT NULL
)
RETURNS TABLE (severity INTEGER, accidentes BIGINT)
LANGUAGE sql STABLE
AS $$
    SELECT a.severity, COUNT(1) AS accidentes
    FROM accidents a
    JOIN cities c ON c.city_id = a.city_id
    WHERE (p_ini    IS NULL OR a.start_time_utc >= p_ini)
      AND (p_fin    IS NULL OR a.start_time_utc <= p_fin)
      AND (p_estado IS NULL OR c.state_code = p_estado)
    GROUP BY a.severity
    ORDER BY a.severity
$$;
