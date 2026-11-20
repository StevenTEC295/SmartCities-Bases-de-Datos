-- Esquema de sensores. Se ejecuta solo la primera vez que se crea el volumen de PostgreSQL.

CREATE TABLE Sensors_table (
    dev_id    VARCHAR(12) PRIMARY KEY,
    name      VARCHAR(29) NOT NULL,
    latitude  DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL
);
