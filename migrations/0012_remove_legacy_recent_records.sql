-- This will delete all recent weather records by cascade
DELETE FROM sources WHERE observation_type = 'recent';
