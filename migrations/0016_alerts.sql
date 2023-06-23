CREATE TYPE alert_category AS ENUM ('met', 'health');
CREATE TYPE alert_response_type AS ENUM ('prepare', 'allclear', 'none', 'monitor');
CREATE TYPE alert_urgency AS ENUM ('immediate', 'future');
CREATE TYPE alert_severity AS ENUM ('minor', 'moderate', 'severe', 'extreme');
CREATE TYPE alert_certainty AS ENUM ('observed', 'likely');

CREATE TABLE alerts (
  id                serial PRIMARY KEY,
  alert_id          varchar(255) NOT NULL,
  effective         timestamptz NOT NULL,
  onset             timestamptz NOT NULL,
  expires           timestamptz,
  category          alert_category,
  response_type     alert_response_type,
  urgency           alert_urgency,
  severity          alert_severity,
  certainty         alert_certainty,
  event_code        smallint CHECK (event_code BETWEEN 11 AND 248),
  event_en          text,
  event_de          text,
  headline_en       text NOT NULL,
  headline_de       text NOT NULL,
  description_en    text NOT NULL,
  description_de    text NOT NULL,
  instruction_en    text,
  instruction_de    text,

  CONSTRAINT alerts_key UNIQUE (alert_id)
);

CREATE TABLE alert_cells (
  alert_id       int NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
  warn_cell_id   int NOT NULL,

  CONSTRAINT alert_cells_key UNIQUE (warn_cell_id, alert_id)
);
