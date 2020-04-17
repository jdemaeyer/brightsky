CREATE TABLE migrations (
  id       integer PRIMARY KEY,
  name     varchar(255),
  applied  timestamptz DEFAULT current_timestamp
);
