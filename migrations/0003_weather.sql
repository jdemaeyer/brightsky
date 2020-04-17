CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TYPE observation_type AS ENUM ('historical', 'recent', 'current', 'forecast');

CREATE TABLE sources (
  id                serial PRIMARY KEY,
  station_id        varchar(5) NOT NULL,
  observation_type  observation_type NOT NULL,
  location          geography(POINT) NOT NULL,
  height            smallint NOT NULL,

  CONSTRAINT weather_source_key UNIQUE (station_id, observation_type, location, height)
);

CREATE TABLE weather (
  timestamp       timestamptz NOT NULL,
  source_id       int NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  precipitation   real CHECK (precipitation >= 0),
  pressure_msl    integer CHECK (pressure_msl > 0),
  sunshine        smallint CHECK (sunshine BETWEEN 0 and 3600),
  temperature     real CHECK (temperature > 0),
  wind_direction  smallint CHECK (wind_direction BETWEEN 0 AND 360),
  wind_speed      real CHECK (wind_speed >= 0),

  CONSTRAINT weather_key UNIQUE (timestamp, source_id)
);
