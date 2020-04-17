CREATE TABLE parsed_files (
  url            text PRIMARY KEY,
  last_modified  timestamptz NOT NULL,
  file_size      integer NOT NULL,
  parsed_at      timestamptz NOT NULL
);
