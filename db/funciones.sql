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

-- Estado de cada sensor (uno por sensor fisico) en el periodo [p_ini, p_fin].
-- Activo = tiene mediciones en el periodo (su rango de fechas de datos se traslapa con el periodo).
-- Inactivo = no tiene mediciones en el periodo, o nunca ha tenido (fechas nulas).
CREATE OR REPLACE FUNCTION estado_sensores(
    p_ini TIMESTAMP,
    p_fin TIMESTAMP
)
RETURNS TABLE (
    fuente             TEXT,
    sensor_id          TEXT,
    nombre             TEXT,
    parametros         TEXT,
    zona               TEXT,
    fecha_inicio_datos TIMESTAMP,
    fecha_fin_datos    TIMESTAMP,
    estado             TEXT
)
LANGUAGE sql STABLE
AS $$
    SELECT v.fuente,
           v.sensor_id,
           v.nombre,
           string_agg(v.parametro, ', ' ORDER BY v.parametro) AS parametros,
           v.zona,
           v.fecha_inicio_datos,
           v.fecha_fin_datos,
           CASE WHEN v.fecha_inicio_datos <= p_fin AND v.fecha_fin_datos >= p_ini
                THEN 'activo' ELSE 'inactivo' END AS estado
    FROM sensores_unificados v
    GROUP BY v.fuente, v.sensor_id, v.nombre, v.zona, v.fecha_inicio_datos, v.fecha_fin_datos
    ORDER BY v.fuente, v.nombre, v.sensor_id
$$;
