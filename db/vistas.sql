-- Vista unica de sensores para el dashboard.
-- No duplica datos: se calcula en cada consulta a partir de las tablas base, que conservan
-- sus claves primarias y foraneas (sensores -> locations, sensores -> parametros, sensores -> organismos).


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
    o.nombre                      AS organismo
FROM sensores s
JOIN locations  l ON l.location_id  = s.location_id
JOIN parametros p ON p.parameter_id = s.parameter_id
JOIN organismos o ON o.organismo_id = s.organismo_id

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
    o.nombre
FROM sensors_table c
JOIN organismos o ON o.organismo_id = c.organismo_id
CROSS JOIN (VALUES
    ('temperature', 'C'),
    ('humidity',    '%'),
    ('battery',     'V')
) AS m(parametro, unidad);
