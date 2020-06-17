ALTER TYPE observation_type ADD VALUE 'synop' BEFORE 'forecast';

CREATE TABLE synop (
  timestamp               timestamptz NOT NULL,
  source_id               int NOT NULL REFERENCES sources(id) ON DELETE CASCADE,

  cloud_cover             smallint CHECK (cloud_cover BETWEEN 0 and 100),
  dew_point               real CHECK (dew_point > 0),
  precipitation_10        real CHECK (precipitation_10 >= 0),
  precipitation_30        real CHECK (precipitation_30 >= 0),
  precipitation_60        real CHECK (precipitation_60 >= 0),
  pressure_msl            integer CHECK (pressure_msl > 0),
  relative_humidity       smallint CHECK (relative_humidity BETWEEN 0 and 100),
  sunshine_10             smallint CHECK (sunshine_10 BETWEEN 0 and 600),
  sunshine_30             smallint CHECK (sunshine_30 BETWEEN 0 and 1800),
  sunshine_60             smallint CHECK (sunshine_60 BETWEEN 0 and 3600),
  temperature             real CHECK (temperature > 0),
  visibility              int CHECK (visibility >= 0),
  wind_direction_10       smallint CHECK (wind_direction_10 BETWEEN 0 AND 360),
  wind_direction_30       smallint CHECK (wind_direction_30 BETWEEN 0 AND 360),
  wind_direction_60       smallint CHECK (wind_direction_60 BETWEEN 0 AND 360),
  wind_speed_10           real CHECK (wind_speed_10 >= 0),
  wind_speed_30           real CHECK (wind_speed_30 >= 0),
  wind_speed_60           real CHECK (wind_speed_60 >= 0),
  wind_gust_direction_10  smallint CHECK (wind_gust_direction_10 BETWEEN 0 AND 360),
  wind_gust_direction_30  smallint CHECK (wind_gust_direction_30 BETWEEN 0 AND 360),
  wind_gust_direction_60  smallint CHECK (wind_gust_direction_60 BETWEEN 0 AND 360),
  wind_gust_speed_10      real CHECK (wind_gust_speed_10 >= 0),
  wind_gust_speed_30      real CHECK (wind_gust_speed_30 >= 0),
  wind_gust_speed_60      real CHECK (wind_gust_speed_60 >= 0),

  CONSTRAINT synop_key UNIQUE (timestamp, source_id)
);
