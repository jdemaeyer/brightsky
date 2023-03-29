ALTER TABLE weather
  ADD COLUMN precipitation_probability    smallint CHECK (precipitation_probability BETWEEN 0 and 100),
  ADD COLUMN precipitation_probability_6h smallint CHECK (precipitation_probability_6h BETWEEN 0 and 100);
