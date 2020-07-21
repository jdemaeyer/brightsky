CREATE TYPE weather_condition AS ENUM (
  'dry',
  'fog',
  'rain',
  'sleet',
  'snow',
  'hail',
  'thunderstorm'
);

ALTER TABLE weather ADD COLUMN condition weather_condition;
ALTER TABLE synop ADD COLUMN condition weather_condition;

-- Re-parse all precipitation records
DELETE FROM parsed_files WHERE url ILIKE '%stundenwerte_RR_%';

-- Same query as before with condition field added
DROP MATERIALIZED VIEW current_weather;
CREATE MATERIALIZED VIEW current_weather AS
  WITH last_timestamp AS (
    SELECT
      source_id,
      MAX(timestamp) AS last_timestamp
    FROM synop
    GROUP BY source_id
  )
  SELECT
    last_timestamp.source_id,
    last_timestamp.last_timestamp AS timestamp,
    latest.cloud_cover,
    latest.condition,
    latest.dew_point,
    latest.precipitation_10,
    last_half_hour.precipitation_30,
    last_hour.precipitation_60,
    latest.pressure_msl,
    latest.relative_humidity,
    latest.visibility,
    latest.wind_direction_10,
    last_half_hour.wind_direction_30,
    last_hour.wind_direction_60,
    latest.wind_speed_10,
    last_half_hour.wind_speed_30,
    last_hour.wind_speed_60,
    latest.wind_gust_direction_10,
    last_half_hour.wind_gust_direction_30,
    last_hour.wind_gust_direction_60,
    latest.wind_gust_speed_10,
    last_half_hour.wind_gust_speed_30,
    last_hour.wind_gust_speed_60,
    sunshine.sunshine_30,
    sunshine.sunshine_60,
    latest.temperature
  FROM last_timestamp
  JOIN (
    SELECT
      source_id,
      LAST(cloud_cover ORDER BY timestamp) AS cloud_cover,
      LAST(condition ORDER BY timestamp) AS condition,
      LAST(dew_point ORDER BY timestamp) AS dew_point,
      LAST(precipitation_10 ORDER BY timestamp) AS precipitation_10,
      LAST(pressure_msl ORDER BY timestamp) AS pressure_msl,
      LAST(relative_humidity ORDER BY timestamp) AS relative_humidity,
      LAST(visibility ORDER BY timestamp) AS visibility,
      LAST(wind_direction_10 ORDER BY timestamp) AS wind_direction_10,
      LAST(wind_speed_10 ORDER BY timestamp) AS wind_speed_10,
      LAST(wind_gust_direction_10 ORDER BY timestamp) AS wind_gust_direction_10,
      LAST(wind_gust_speed_10 ORDER BY timestamp) AS wind_gust_speed_10,
      LAST(temperature ORDER BY timestamp) AS temperature
    FROM synop s
    WHERE timestamp >= now() - '90 minutes'::interval
    GROUP BY source_id
  ) latest ON last_timestamp.source_id = latest.source_id
  LEFT JOIN (
    SELECT
      synop.source_id,
      round(AVG(precipitation_10) * 6 * 100) / 100 AS precipitation_60,
      round(AVG(wind_speed_10) * 10) / 10 AS wind_speed_60,
      (round(atan2d(AVG(sind(wind_direction_10)), AVG(cosd(wind_direction_10))))::int + 360) % 360 AS wind_direction_60,
      MAX(wind_gust_speed_10) AS wind_gust_speed_60,
      LAST(wind_gust_direction_10 ORDER BY wind_gust_speed_10) AS wind_gust_direction_60
    FROM synop
    JOIN last_timestamp ON synop.source_id = last_timestamp.source_id
    WHERE timestamp > last_timestamp - '60 minutes'::interval
    GROUP BY synop.source_id
  ) last_hour ON latest.source_id = last_hour.source_id
  LEFT JOIN (
    SELECT
      synop.source_id,
      round(AVG(precipitation_10) * 3 * 100) / 100 AS precipitation_30,
      round(AVG(wind_speed_10) * 10) / 10 AS wind_speed_30,
      (round(atan2d(AVG(sind(wind_direction_10)), AVG(cosd(wind_direction_10))))::int + 360) % 360 AS wind_direction_30,
      MAX(wind_gust_speed_10) AS wind_gust_speed_30,
      LAST(wind_gust_direction_10 ORDER BY wind_gust_speed_10) AS wind_gust_direction_30
    FROM synop
    JOIN last_timestamp ON synop.source_id = last_timestamp.source_id
    WHERE timestamp > last_timestamp - '30 minutes'::interval
    GROUP BY synop.source_id
  ) last_half_hour ON latest.source_id = last_half_hour.source_id
  LEFT JOIN (
    SELECT
      s30_latest.source_id,
      CASE
        WHEN s30_latest.timestamp > s60.timestamp THEN s30_latest.sunshine_30
        ELSE s60.sunshine_60 - s30_latest.sunshine_30
      END AS sunshine_30,
      CASE
        WHEN s30_latest.timestamp > s60.timestamp THEN s30_latest.sunshine_30 + s60.sunshine_60 - s30_previous.sunshine_30
        ELSE s60.sunshine_60
      END AS sunshine_60
    FROM (
      SELECT DISTINCT ON (source_id) source_id, timestamp, sunshine_30
      FROM synop
      WHERE sunshine_30 IS NOT NULL
      ORDER BY source_id, timestamp DESC
    ) s30_latest
    JOIN (
      SELECT source_id, timestamp, sunshine_30
      FROM synop
    ) s30_previous ON
      s30_latest.source_id = s30_previous.source_id AND
      s30_previous.timestamp = s30_latest.timestamp - '1 hour'::interval
    JOIN (
      SELECT DISTINCT ON (source_id) source_id, timestamp, sunshine_60
      FROM synop
      WHERE sunshine_60 IS NOT NULL
      ORDER BY source_id, timestamp DESC
    ) s60 ON
      s30_previous.source_id = s60.source_id AND
      s60.timestamp > s30_previous.timestamp
  ) sunshine ON latest.source_id = sunshine.source_id
  ORDER BY latest.source_id;

CREATE UNIQUE INDEX current_weather_key ON current_weather (source_id);
