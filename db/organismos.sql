-- Organismos responsables (origen de cada dataset) y su relacion con los sensores.


CREATE TABLE IF NOT EXISTS organismos (
    organismo_id SMALLINT PRIMARY KEY,
    nombre       TEXT NOT NULL UNIQUE
);

INSERT INTO organismos (organismo_id, nombre) VALUES
    (1, 'OpenAQ'),
    (2, 'Victorian Government, Australia'),
    (3, 'Gobierno de los Estados Unidos')   -- origen del dataset de accidentes 
ON CONFLICT DO NOTHING;

-- El valor por defecto permite que los notebooks sigan cargando sensores sin cambios:
-- todo sensor de contaminacion es de OpenAQ (1) y todo sensor de clima es del gobierno de Victoria (2).
ALTER TABLE sensores
    ADD COLUMN IF NOT EXISTS organismo_id SMALLINT NOT NULL DEFAULT 1 REFERENCES organismos(organismo_id);

ALTER TABLE sensors_table
    ADD COLUMN IF NOT EXISTS organismo_id SMALLINT NOT NULL DEFAULT 2 REFERENCES organismos(organismo_id);
