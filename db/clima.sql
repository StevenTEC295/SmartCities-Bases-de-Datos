-- Esquema de sensores

CREATE TABLE Sensors_table (
    dev_id    VARCHAR(12) PRIMARY KEY,
    name      VARCHAR(29) NOT NULL,
    latitude  DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);
