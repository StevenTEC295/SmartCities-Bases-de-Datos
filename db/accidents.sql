CREATE TABLE sources (
    source_id SMALLINT PRIMARY KEY,
    source    TEXT NOT NULL UNIQUE
);

CREATE TABLE states (
    state_code TEXT PRIMARY KEY
);

CREATE TABLE cities (
    city_id    INTEGER PRIMARY KEY,
    city       TEXT NOT NULL,
    county     TEXT NOT NULL,
    state_code TEXT NOT NULL REFERENCES states(state_code),
    UNIQUE (city, county, state_code)
);

CREATE TABLE accidents (
    id                    TEXT PRIMARY KEY,
    source_id             SMALLINT         NOT NULL REFERENCES sources(source_id),
    severity              INTEGER          NOT NULL,
    start_time            TIMESTAMP        NOT NULL,
    end_time              TIMESTAMP        NOT NULL,
    start_time_utc        TIMESTAMP        NOT NULL,
    end_time_utc          TIMESTAMP        NOT NULL,
    start_lat             DOUBLE PRECISION NOT NULL,
    start_lng             DOUBLE PRECISION NOT NULL,
    distance_mi           DOUBLE PRECISION NOT NULL,
    description           TEXT             NOT NULL,
    street                TEXT             NOT NULL,
    city_id               INTEGER          NOT NULL REFERENCES cities(city_id),
    timezone              TEXT             NOT NULL,
    sunrise_sunset        TEXT             NOT NULL,
    civil_twilight        TEXT             NOT NULL,
    nautical_twilight     TEXT             NOT NULL,
    astronomical_twilight TEXT             NOT NULL
);

CREATE TABLE accident_road_features (
    id              TEXT PRIMARY KEY REFERENCES accidents(id),
    amenity         BOOLEAN NOT NULL,
    bump            BOOLEAN NOT NULL,
    crossing        BOOLEAN NOT NULL,
    give_way        BOOLEAN NOT NULL,
    junction        BOOLEAN NOT NULL,
    no_exit         BOOLEAN NOT NULL,
    railway         BOOLEAN NOT NULL,
    roundabout      BOOLEAN NOT NULL,
    station         BOOLEAN NOT NULL,
    stop            BOOLEAN NOT NULL,
    traffic_calming BOOLEAN NOT NULL,
    traffic_signal  BOOLEAN NOT NULL,
    turning_loop    BOOLEAN NOT NULL
);