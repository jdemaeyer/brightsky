ALTER TABLE sources
  ADD COLUMN first_record timestamptz,
  ADD COLUMN last_record timestamptz;

UPDATE sources SET
  first_record = record_range.first_record,
  last_record = record_range.last_record
FROM (
  SELECT
    source_id,
    MIN(timestamp) AS first_record,
    MAX(timestamp) AS last_record
  FROM weather
  GROUP BY source_id
  UNION
  SELECT
    source_id,
    MIN(timestamp) AS first_record,
    MAX(timestamp) AS last_record
  FROM synop
  GROUP BY source_id
) AS record_range
WHERE sources.id = record_range.source_id;
