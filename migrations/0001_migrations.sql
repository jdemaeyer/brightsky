CREATE TABLE migrations (
  id       integer PRIMARY KEY,
  name     varchar(255),
  applied  timestamp with time zone DEFAULT current_timestamp
);
