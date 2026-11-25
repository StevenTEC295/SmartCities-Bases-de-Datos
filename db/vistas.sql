-- Vista unica de sensores para el dashboard.
-- No duplica datos: se calcula en cada consulta a partir de las tablas base, que conservan
-- sus claves primarias y foraneas (sensores -> locations, sensores -> parametros, sensores -> organismos).
-- Zonas: contaminacion usa el estado de su estacion; clima (Australia) se asigna de forma SIMULADA,
-- rotando los dispositivos (ordenados por dev_id) entre las zonas. zona_simulada lo indica.


CREATE OR REPLACE VIEW sensores_unificados AS
SELECT
    'contaminacion'::text         AS fuente,
    s.sensor_id::text             AS sensor_id,
    l.location_name               AS nombre,
    l.latitude                    AS latitude,
    l.longitude                   AS longitude,
    s.location_id                 AS location_id, -- se conserva la referencia a locations
    s.parameter_id                AS parameter_id, -- se conserva la referencia a parametros
    p.parameter                   AS parametro,
    p.unit                        AS unidad,
    s.organismo_id                AS organismo_id, -- se conserva la referencia a organismos
    o.nombre                      AS organismo,
    z.zona_id                     AS zona_id,
    z.nombre                      AS zona,
    z.region                      AS region,
    FALSE                         AS zona_simulada,
    s.fecha_inicio_datos          AS fecha_inicio_datos,
    s.fecha_fin_datos             AS fecha_fin_datos
FROM sensores s
JOIN locations  l ON l.location_id  = s.location_id
JOIN parametros p ON p.parameter_id = s.parameter_id
JOIN organismos o ON o.organismo_id = s.organismo_id
LEFT JOIN estados_zona ez ON ez.state_code = l.state_code
LEFT JOIN zonas z ON z.zona_id = ez.zona_id

UNION ALL

SELECT
    'clima'::text                 AS fuente,
    c.dev_id::text                AS sensor_id,
    c.name::text                  AS nombre,
    c.latitude,
    c.longitude,
    NULL::integer                 AS location_id,
    NULL::integer                 AS parameter_id,
    m.parametro,
    m.unidad,
    c.organismo_id,
    o.nombre,
    z.zona_id,
    z.nombre,
    z.region,
    TRUE                          AS zona_simulada,
    c.fecha_inicio_datos,
    c.fecha_fin_datos
FROM (
    SELECT t.dev_id, t.name, t.latitude, t.longitude, t.organismo_id,
           t.fecha_inicio_datos, t.fecha_fin_datos,
           1 + ((ROW_NUMBER() OVER (ORDER BY t.dev_id) - 1) % (SELECT COUNT(1) FROM zonas)) AS zona_asignada
    FROM sensors_table t
) c
JOIN organismos o ON o.organismo_id = c.organismo_id
LEFT JOIN zonas z ON z.zona_id = c.zona_asignada
CROSS JOIN (VALUES
    ('temperature', 'C'),
    ('humidity',    '%'),
    ('battery',     'V')
) AS m(parametro, unidad);
