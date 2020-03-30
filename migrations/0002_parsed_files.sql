CREATE TABLE parsed_files (
  url            text PRIMARY KEY,
  last_modified  timestamp with time zone NOT NULL,
  file_size      integer NOT NULL,
  parsed_at      timestamp with time zone NOT NULL
);
