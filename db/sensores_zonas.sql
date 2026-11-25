-- Zonas del proyecto: las 9 divisiones del Censo de EE. UU. (agrupadas en 4 regiones).
-- Sirven para relacionar los datasets: accidentes (ciudad -> estado -> zona), estaciones de
-- contaminacion (locations.state_code -> estado -> zona) y clima de Australia (asignacion simulada
-- en la vista sensores_unificados).
-- Idempotente: se puede ejecutar varias veces.

CREATE TABLE IF NOT EXISTS zonas (
    zona_id SMALLINT PRIMARY KEY,
    nombre  TEXT NOT NULL UNIQUE,
    region  TEXT NOT NULL
);

INSERT INTO zonas (zona_id, nombre, region) VALUES
    (1, 'New England',        'Northeast'),
    (2, 'Mid-Atlantic',       'Northeast'),
    (3, 'East North Central', 'Midwest'),
    (4, 'West North Central', 'Midwest'),
    (5, 'South Atlantic',     'South'),
    (6, 'East South Central', 'South'),
    (7, 'West South Central', 'South'),
    (8, 'Mountain',           'West'),
    (9, 'Pacific',            'West')
ON CONFLICT DO NOTHING;

-- Estado -> zona (50 estados + DC). Tabla estatica: no depende de los notebooks.
CREATE TABLE IF NOT EXISTS estados_zona (
    state_code TEXT PRIMARY KEY,
    zona_id    SMALLINT NOT NULL REFERENCES zonas(zona_id)
);

INSERT INTO estados_zona (state_code, zona_id) VALUES
    ('CT',1),('ME',1),('MA',1),('NH',1),('RI',1),('VT',1),
    ('NJ',2),('NY',2),('PA',2),
    ('IL',3),('IN',3),('MI',3),('OH',3),('WI',3),
    ('IA',4),('KS',4),('MN',4),('MO',4),('NE',4),('ND',4),('SD',4),
    ('DE',5),('DC',5),('FL',5),('GA',5),('MD',5),('NC',5),('SC',5),('VA',5),('WV',5),
    ('AL',6),('KY',6),('MS',6),('TN',6),
    ('AR',7),('LA',7),('OK',7),('TX',7),
    ('AZ',8),('CO',8),('ID',8),('MT',8),('NV',8),('NM',8),('UT',8),('WY',8),
    ('AK',9),('CA',9),('HI',9),('OR',9),('WA',9)
ON CONFLICT DO NOTHING;

-- Estado de cada estacion de contaminacion. Los notebooks lo escriben al cargar locations;
-- el UPDATE sirve para bases que ya tienen las estaciones cargadas (no hace nada si estan vacias).
ALTER TABLE locations ADD COLUMN IF NOT EXISTS state_code TEXT REFERENCES estados_zona(state_code);

UPDATE locations SET state_code = v.state_code
FROM (VALUES (448, 'MA'), (626, 'NY'), (739, 'DC'), (2139, 'MD')) AS v(location_id, state_code)
WHERE locations.location_id = v.location_id AND locations.state_code IS NULL;

-- Fecha de la primera y de la ultima medicion de cada sensor.
-- Las mediciones viven en MongoDB: los notebooks calculan MIN/MAX y las escriben aqui al cargar.
-- Se usan para sensores instalados por anio, activos/inactivos y sin actividad.
ALTER TABLE sensores      ADD COLUMN IF NOT EXISTS fecha_inicio_datos TIMESTAMP;
ALTER TABLE sensores      ADD COLUMN IF NOT EXISTS fecha_fin_datos    TIMESTAMP;
ALTER TABLE sensors_table ADD COLUMN IF NOT EXISTS fecha_inicio_datos TIMESTAMP;
ALTER TABLE sensors_table ADD COLUMN IF NOT EXISTS fecha_fin_datos    TIMESTAMP;
