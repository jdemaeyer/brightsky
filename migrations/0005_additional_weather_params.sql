ALTER TABLE weather
  ADD COLUMN cloud_cover          smallint CHECK (cloud_cover BETWEEN 0 and 100),
  ADD COLUMN dew_point            real CHECK (dew_point > 0),
  ADD COLUMN relative_humidity    smallint CHECK (relative_humidity BETWEEN 0 and 100),
  ADD COLUMN visibility           int CHECK (visibility >= 0),
  ADD COLUMN wind_gust_direction  smallint CHECK (wind_gust_direction BETWEEN 0 AND 360),
  ADD COLUMN wind_gust_speed      real CHECK (wind_gust_speed >= 0);

-- relative_humidity is parsed from air temperature files
DELETE FROM parsed_files WHERE url ILIKE '%stundenwerte_TU_%'
