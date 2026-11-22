-- Vista unica de sensores para el dashboard.
-- No duplica datos: se calcula en cada consulta a partir de las tablas base, que conservan
-- sus claves primarias y foraneas (sensores -> locations, sensores -> parametros).
-- Se ejecuta solo la primera vez que se crea el volumen de PostgreSQL (despues de las tablas, por orden alfabetico).
-- Una fila por sensor y parametro medido. Clave de la vista: (fuente, sensor_id, parametro).

CREATE OR REPLACE VIEW sensores_unificados AS
SELECT
    'contaminacion'::text         AS fuente,
    s.sensor_id::text             AS sensor_id,
    l.location_name               AS nombre,
    NULL::double precision        AS latitude,    -- el CSV de contaminacion no trae coordenadas
    NULL::double precision        AS longitude,
    s.location_id                 AS location_id, -- se conserva la referencia a locations
    s.parameter_id                AS parameter_id, -- se conserva la referencia a parametros
    p.parameter                   AS parametro,
    p.unit                        AS unidad
FROM sensores s
JOIN locations  l ON l.location_id  = s.location_id
JOIN parametros p ON p.parameter_id = s.parameter_id

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
    m.unidad
FROM sensors_table c
CROSS JOIN (VALUES
    ('temperature', 'C'),   -- unidades asumidas, confirmar con la fuente del dataset
    ('humidity',    '%'),
    ('battery',     'V')
) AS m(parametro, unidad);
