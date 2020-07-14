# Bright Sky

JSON API for DWD's open weather data.


### Looking for something specific?

#### I just want to retrieve some weather data

You can use the free [public Bright Sky instance](https://brightsky.dev/)!

#### I want to run my own instance of Bright Sky

Check out the [infrastructure
repo](https://github.com/jdemaeyer/brightsky-infrastructure/)!

#### I want to play around with or contribute to Bright Sky's source code

Read on. :)


## Quickstart

Just run `docker-compose up` and you should be good to go. This will set up a
PostgreSQL database (with persistent storage in `.data`), run a Redis server,
and start the Bright Sky worker and webserver. The worker periodically polls
the DWD Open Data Server for updates, parses them, and stores them in the
database. The webserver will be listening to API requests on port 5000.


## Overview

Bright Sky is a rather simple project consisting of four components:

 * The `brightsky` worker, which leverages the logic contained in the
   `brightsky` Python package to retrieve weather records from the DWD server,
   parse them, and store them in a database. It will periodically poll the DWD
   servers for new data.

 * The `brightsky` webserver, which serves as gate to our database and
   processes all queries for weather records coming from the outside world.

 * A PostgreSQL database consisting of two relevant tables:

    * `sources` contains information on the locations for which we hold weather
      records, and
    * `weather` contains the history of actual meteorological measurements (or
      forecasts) for these locations.

   The database structure can be set up by running the `migrate` command, which
   will simply apply all `.sql` files found in the `migrations` folder.

 * A Redis server, which is used as the backend of the worker's task queue.

Most of the tasks performed by the worker and webserver can also be performed
independently. Run `docker-compose run --rm brightsky` to get a list of
available commands.


## Hacking

Constantly rebuilding the `brightsky` container while working on the code can
become cumbersome, and the default setting of parsing records dating all the
way back to 2010 will make your development database unnecessarily large. You
can set up a more lightweight development environment as follows:

 1. Create a virtual environment and install our dependencies:
    `python -m virtualenv .venv && source .venv/bin/activate && pip install -r
    requirements.txt && pip install -e .`

 2. Start a PostgreSQL container:
    `docker-compose run --rm -p 5432:5432 postgres`

 3. Start a Redis container:
    `docker-compose run --rm -p 6379:6379 redis`

 4. Point `brightsky` to your containers, and configure a tighter date
    threshold for parsing DWD data, by adding the following `.env` file:
    ```
    BRIGHTSKY_DATABASE_URL=postgres://postgres:pgpass@localhost
    BRIGHTSKY_BENCHMARK_DATABASE_URL=postgres://postgres:pgpass@localhost/benchmark
    BRIGHTSKY_REDIS_URL=redis://localhost
    BRIGHTSKY_MIN_DATE=2020-01-01
    ```

You should now be able to directly run `brightsky` commands via `python -m
brightsky`, and changes to the source code should be effective immediately.
