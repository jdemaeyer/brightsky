CREATE TYPE alert_status AS ENUM ('actual', 'test');

ALTER TABLE alerts ADD COLUMN status alert_status NOT NULL DEFAULT 'actual';
ALTER TABLE alerts ALTER COLUMN status DROP DEFAULT;
