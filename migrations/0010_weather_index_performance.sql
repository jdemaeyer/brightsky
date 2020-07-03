ALTER TABLE weather
  DROP CONSTRAINT weather_key,
  ADD CONSTRAINT weather_key UNIQUE (source_id, timestamp);
