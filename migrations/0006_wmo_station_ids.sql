-- There are a bunch of stations only differing in their DWD station ID but not
-- their observation_type or location. We'll have to decide how to deal with
-- these during parsing.
DELETE
  FROM sources s
  USING sources s2
  WHERE
    s.observation_type = s2.observation_type AND
    s.lat = s2.lat AND
    s.lon = s2.lon AND
    s.height = s2.height AND
    s.id > s2.id

ALTER TABLE sources
  RENAME COLUMN station_id TO dwd_station_id;
ALTER TABLE sources
  ALTER COLUMN dwd_station_id DROP NOT NULL,
  ADD COLUMN wmo_station_id varchar(5),
  DROP CONSTRAINT weather_source_key,
  ADD CONSTRAINT weather_source_key UNIQUE (observation_type, lat, lon, height);
