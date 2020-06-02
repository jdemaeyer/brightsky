ALTER TABLE sources ADD COLUMN station_name varchar(255) NOT NULL DEFAULT '';
ALTER TABLE sources ALTER COLUMN station_name DROP DEFAULT;

-- Force full re-parse
DELETE FROM parsed_files;
