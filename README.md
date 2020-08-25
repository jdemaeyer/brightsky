# Bright Sky

[![API Status](https://img.shields.io/website?down_message=offline&label=api&up_message=online&url=https%3A%2F%2Fapi.brightsky.dev%2F)](https://api.brightsky.dev/)
[![Docs Status](https://img.shields.io/website?down_message=offline&label=docs&up_message=online&url=https%3A%2F%2Fbrightsky.dev%2Fdocs%2F)](https://brightsky.dev/docs/)
[![Build Status](https://img.shields.io/github/workflow/status/jdemaeyer/brightsky/Continuous%20Integration)](https://github.com/jdemaeyer/brightsky/actions)
[![PyPI Release](https://img.shields.io/pypi/v/brightsky)](https://pypi.org/project/brightsky/)
[![Docker Hub Release](https://img.shields.io/docker/v/jdemaeyer/brightsky/latest?label=docker)](https://hub.docker.com/r/jdemaeyer/brightsky)

### JSON API for DWD's open weather data.

The DWD ([Deutscher Wetterdienst](https://www.dwd.de/)), as Germany's
meteorological service, publishes a myriad of meteorological observations and
calculations as part of their [Open Data
program](https://www.dwd.de/DE/leistungen/opendata/opendata.html).

[**Bright Sky**](https://brightsky.dev/) is an open-source project aiming to
make some of the more popular data — in particular weather observations from
the DWD station network and weather forecasts from the MOSMIX model — available
in a free, simple JSON API.


### Looking for something specific?

#### I just want to retrieve some weather data

You can use the free [public Bright Sky instance](https://brightsky.dev/)!

#### I want to run my own instance of Bright Sky

Check out the [infrastructure
repo](https://github.com/jdemaeyer/brightsky-infrastructure/)!

#### I want to parse DWD weather files or contribute to Bright Sky's source code

Read on. :)


## Quickstart

There are three main ways to use this package:

* parsing DWD files from the command line,
* parsing DWD files from Python code, or
* running a complete API instance.


### Stand-alone DWD file parsing

1. Install the `brightsky` package from PyPI:
   ```
   pip install brightsky
   ```

2. Call Bright Sky's `parse` command, providing your target file either via
   `--path` (if it is a local file):
   ```
   python -m brightsky parse --path stundenwerte_TU_01766_akt.zip
   ```
   or `--url` (if the file should be downloaded first):
   ```
   python -m brightsky parse --url https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/stundenwerte_TU_01766_akt.zip
   ```

This will output a newline-separated list of JSON records. Note that all
numerical weather data will be converted to SI units.


### Parsing DWD files from Python code

1. Install the `brightsky` package from PyPI:
   ```
   pip install brightsky
   ```

2. In Python, import one of the `brightsky` parsers (or the `get_parser`
   utility function) from `brightsky.parsers`, initialize it with `path` or
   `url`, and call it's `parse()` method. This will return an iterable over
   weather records:
   ```python
   DWD_FILE_URL = (
       'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
       'climate/hourly/air_temperature/recent/stundenwerte_TU_01766_akt.zip')

   # Either auto-detect the correct parser from the filename
   from brightsky.parsers import get_parser
   parser_class = get_parser(DWD_FILE_URL.split('/')[-1])
   parser = parser_class(url=DWD_FILE_URL)

   # Or pick the parser class yourself
   from brightsky.parsers import TemperatureObservationsParser
   parser = TemperatureObservationsParser(url=DWD_FILE_URL)


   parser.download()  # Not necessary if you supply a local path
   for record in parser.parse():
       print(record)
   parser.cleanup()  # If you wish to delete any downloaded files
   ```


### Running a full-fledged instance

_Note: These instructions are aimed at running a Bright Sky instance for
development and testing. Check out our [infrastructure
repository](https://github.com/jdemaeyer/brightsky-infrastructure/) if you want
to set up a production-level API instance._

Just run `docker-compose up` and you should be good to go. This will set up a
PostgreSQL database (with persistent storage in `.data`), run a Redis server,
and start the Bright Sky worker and webserver. The worker periodically polls
the DWD Open Data Server for updates, parses them, and stores them in the
database. The webserver will be listening to API requests on port 5000.


## Architecture

![Bright Sky's Architecture](docs/img/architecture.svg)

Bright Sky is a rather simple project consisting of four components:

 * The `brightsky` worker, which leverages the logic contained in the
   `brightsky` Python package to retrieve weather records from the DWD server,
   parse them, and store them in a database. It will periodically poll the DWD
   servers for new data.

 * The `brightsky` webserver (API), which serves as gate to our database and
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


### Tests

Large parts of our test suite run against a real Postgres database. By default,
these tests will be skipped. To enable them, make sure the
`BRIGHTSKY_TEST_DATABASE_URL` environment variable is set when calling `tox`,
e.g. via:
```
BRIGHTSKY_TEST_DATABASE_URL=postgres://postgres:pgpass@localhost/brightsky_test tox
```

Beware that adding this environment variable to your `.env` file will not work
as that file is not read by `tox`. The database will be **dropped and
recreated** on every test run, so don't use your normal Bright Sky database. ;)


## Acknowledgements

Bright Sky's development is boosted by the priceless guidance and support of
the [Open Knowledge Foundation](https://www.okfn.de/)'s [Prototype
Fund](https://prototypefund.de/) program, and is generously funded by Germany's
[Federal Ministry of Education and Research](https://www.bmbf.de/). Obvious as
it may be, it should be mentioned that none of this would be possible without
the painstaking, never-ending effort of the [Deutscher
Wetterdienst](https://www.dwd.de/).

<a href="https://prototypefund.de/"><img src="docs/img/pf.svg" alt="Prototype Fund" height="100"></a>&nbsp;&nbsp;&nbsp;&nbsp;
<a href="https://okfn.de/"><img src="docs/img/okfde.svg" alt="Open Knowledge Foundation Germany" height="100"></a>&nbsp;&nbsp;&nbsp;&nbsp;
<a href="https://www.bmbf.de/"><img src="docs/img/bmbf.svg" alt="Bundesministerium für Bildung und Forschung" height="100"></a>&nbsp;&nbsp;&nbsp;&nbsp;
<a href="https://www.dwd.de/"><img src="docs/img/dwd.svg" alt="Deutscher Wetterdienst" height="100"></a>
