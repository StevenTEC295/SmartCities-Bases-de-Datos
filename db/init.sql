-- Esquema en 3FN. Se ejecuta solo la primera vez que se crea el volumen de PostgreSQL.

CREATE TABLE locations (
    location_id   INTEGER PRIMARY KEY,
    location_name TEXT NOT NULL UNIQUE
);

CREATE TABLE parametros (
    parameter_id INTEGER PRIMARY KEY,
    parameter    TEXT NOT NULL UNIQUE,   -- pm10, pm25, o3
    unit         TEXT NOT NULL
);

CREATE TABLE sensores (
    sensor_id    INTEGER PRIMARY KEY,
    location_id  INTEGER NOT NULL REFERENCES locations(location_id),
    parameter_id INTEGER NOT NULL REFERENCES parametros(parameter_id)
);
