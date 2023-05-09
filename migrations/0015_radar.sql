CREATE TABLE radar (
  timestamp        timestamptz NOT NULL,

  source           varchar(255) NOT NULL,
  precipitation_5  bytea NOT NULL,

  CONSTRAINT radar_key UNIQUE (timestamp)
);
