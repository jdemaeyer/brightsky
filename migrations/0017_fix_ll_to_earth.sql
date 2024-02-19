-- via https://github.com/diogob/activerecord-postgres-earthdistance/issues/30#issuecomment-1657757447
-- Fixed issue seems to have had no impact on performance but produced spurious
-- 'type "earth" does not exist' error messages
ALTER FUNCTION ll_to_earth SET search_path = public;
