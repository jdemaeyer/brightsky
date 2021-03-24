#!/usr/bin/env python

import datetime
import logging
import random
import re
import time
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from contextlib import contextmanager
from functools import lru_cache
from multiprocessing import cpu_count

import click
import psycopg2
from dateutil.tz import tzutc
from falcon.testing import TestClient

from brightsky import db, tasks
from brightsky.settings import settings
from brightsky.utils import configure_logging
from brightsky.web import app


logger = logging.getLogger('benchmark')


@contextmanager
def _time(description, precision=0, unit='s'):
    start = time.time()
    yield
    delta = round(time.time() - start, precision)
    if unit == 'h':
        delta_str = str(datetime.timedelta(seconds=delta))
    else:
        delta_str = '{:{}.{}f} s'.format(delta, precision+4, precision)
    if not description.rstrip().endswith(':'):
        description += ':'
    click.echo(f'{description} {delta_str}')


@lru_cache
def get_client():
    return TestClient(app)


@click.group()
def cli():
    try:
        settings['DATABASE_URL'] = settings.BENCHMARK_DATABASE_URL
    except AttributeError:
        raise click.ClickException(
            'Please set the BRIGHTSKY_BENCHMARK_DATABASE_URL environment '
            'variable')
    # This gives us roughly 100 days of weather records in total:
    # 89 from recent observations, 1 from current observations, 10 from MOSMIX
    settings['MIN_DATE'] = datetime.datetime(2020, 1, 1, tzinfo=tzutc())
    settings['MAX_DATE'] = datetime.datetime(2020, 3, 30, tzinfo=tzutc())


@cli.command(help='Recreate and populate benchmark database')
def build():
    logger.info('Dropping and recreating database')
    db_url_base, db_name = settings.DATABASE_URL.rsplit('/', 1)
    with psycopg2.connect(db_url_base + '/postgres') as conn:
        with conn.cursor() as cur:
            conn.set_isolation_level(0)
            cur.execute('DROP DATABASE IF EXISTS %s' % (db_name,))
            cur.execute('CREATE DATABASE %s' % (db_name,))
    db.migrate()
    file_infos = tasks.poll()
    # Make sure we finish parsing MOSMIX before going ahead as current
    # observations depend on it
    tasks.parse(url=next(file_infos)['url'], export=True)
    with ThreadPoolExecutor(max_workers=2*cpu_count()+1) as executor:
        with _time('Database creation time', unit='h'):
            futures = [
                executor.submit(tasks.parse, url=file_info['url'], export=True)
                for file_info in file_infos]
            finished, pending = wait(futures, return_when=FIRST_EXCEPTION)
            for f in pending:
                f.cancel()
            for f in finished:
                # Re-raise any occured exceptions
                if exc := f.exception():
                    raise exc


@cli.command(help='Calculate database size')
def db_size():
    m = re.search(r'/(\w+)$', settings.DATABASE_URL)
    db_name = m.group(1) if m else 'postgres'
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT pg_database_size(%s)', (db_name,))
            db_size = cur.fetchone()
            table_sizes = {}
            for table in ['weather', 'synop', 'sources']:
                cur.execute('SELECT pg_total_relation_size(%s)', (table,))
                table_sizes[table] = cur.fetchone()[0]
    click.echo('Total database size:\n%6d MB' % (db_size[0] / 1024 / 1024))
    click.echo(
        'Table sizes:\n' + '\n'.join(
            '%6d MB  %s' % (size / 1024 / 1024, table)
            for table, size in table_sizes.items()))


@cli.command(help='Re-parse MOSMIX data')
def mosmix_parse():
    MOSMIX_URL = (
        'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/'
        'all_stations/kml/MOSMIX_S_LATEST_240.kmz')
    with _time('MOSMIX Re-parse', unit='h'):
        tasks.parse(url=MOSMIX_URL, export=True)


def _query_sequential(path, kwargs_list, **base_kwargs):
    client = get_client()
    for kwargs in kwargs_list:
        client.simulate_get(path, params={**base_kwargs, **kwargs})


def _query_parallel(path, kwargs_list, **base_kwargs):
    client = get_client()
    with ThreadPoolExecutor(max_workers=2*cpu_count()+1) as executor:
        for kwargs in kwargs_list:
            executor.submit(
                client.simulate_get, path, params={**base_kwargs, **kwargs})


@cli.command('query', help='Query records from database')
def query_():
    # Generate 50 random locations within Germany's bounding box. Locations
    # and sources will be the same across different runs since we hard-code the
    # PRNG seed.
    random.seed(1)
    location_kwargs = [
        {
            'lat': random.uniform(47.30, 54.98),
            'lon': random.uniform(5.99, 15.02),
        }
        for _ in range(100)]
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dwd_station_id, id
                FROM sources
                WHERE observation_type = %s
                """,
                ('historical',))
            rows = random.choices(cur.fetchall(), k=100)
            station_kwargs = [
                {'dwd_station_id': row['dwd_station_id']} for row in rows]
            source_kwargs = [{'source_id': row['id']} for row in rows]
            cur.execute(
                """
                SELECT MAX(last_record)
                FROM sources
                WHERE observation_type = 'current'
                """)
            today = cur.fetchone()['max'].date().isoformat()
    date = '2020-02-14'
    last_date = '2020-02-21'

    def _test_with_kwargs(kwargs_list):
        with _time('  100  one-day queries, sequential', precision=2):
            _query_sequential('/weather', kwargs_list, date=date)
        with _time('  100  one-day queries, parallel:  ', precision=2):
            _query_parallel('/weather', kwargs_list, date=date)
        with _time('  100 one-week queries, sequential', precision=2):
            _query_sequential(
                '/weather', kwargs_list, date=date, last_date=last_date)
        with _time('  100 one-week queries, parallel:  ', precision=2):
            _query_parallel(
                '/weather', kwargs_list, date=date, last_date=last_date)

    click.echo('Sources by lat/lon:')
    with _time('  100  queries, sequential:        ', precision=2):
        _query_sequential('/sources', location_kwargs)
    with _time('  100  queries, parallel:          ', precision=2):
        _query_parallel('/sources', location_kwargs)
    click.echo('\nSources by station:')
    with _time('  100  queries, sequential:        ', precision=2):
        _query_sequential('/sources', station_kwargs)
    with _time('  100  queries, parallel:          ', precision=2):
        _query_parallel('/sources', station_kwargs)
    click.echo('\nSources by source:')
    with _time('  100  queries, sequential:        ', precision=2):
        _query_sequential('/sources', source_kwargs)
    with _time('  100  queries, parallel:          ', precision=2):
        _query_parallel('/sources', source_kwargs)

    click.echo('\nWeather by lat/lon:')
    _test_with_kwargs(location_kwargs)
    click.echo('\nWeather by lat/lon, today:')
    with _time('  100  one-day queries, sequential', precision=2):
        _query_sequential('/weather', location_kwargs, date=today)
    with _time('  100  one-day queries, parallel:  ', precision=2):
        _query_parallel('/weather', location_kwargs, date=today)
    click.echo('\nWeather by station:')
    _test_with_kwargs(station_kwargs)
    click.echo('\nWeather by source:')
    _test_with_kwargs(source_kwargs)

    click.echo('\nCurrent weather by lat/lon:')
    with _time('  100  queries, sequential:        ', precision=2):
        _query_sequential('/current_weather', location_kwargs)
    with _time('  100  queries, parallel:          ', precision=2):
        _query_parallel('/current_weather', location_kwargs)
    click.echo('\nCurrent weather by station:')
    with _time('  100  queries, sequential:        ', precision=2):
        _query_sequential('/current_weather', station_kwargs)
    with _time('  100  queries, parallel:          ', precision=2):
        _query_parallel('/current_weather', station_kwargs)


if __name__ == '__main__':
    configure_logging()
    cli()
