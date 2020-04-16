#!/usr/bin/env python

import datetime
import logging
import random
import time
from concurrent.futures import as_completed, ThreadPoolExecutor
from contextlib import contextmanager
from multiprocessing import cpu_count

import click
import psycopg2
from dateutil.tz import tzutc

from brightsky import db, query, tasks
from brightsky.settings import settings
from brightsky.utils import configure_logging, load_dotenv


logger = logging.getLogger('benchmark')


@contextmanager
def _time(description, precision=0):
    start = time.time()
    yield
    delta = round(time.time() - start, precision)
    click.echo(
        '%s: %s h' % (description, datetime.timedelta(seconds=delta)))


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
        with _time('Database creation time'):
            futures = [
                executor.submit(tasks.parse, url=file_info['url'], export=True)
                for file_info in file_infos]
            for f in as_completed(futures):
                # Make sure we re-raise any occured exceptions
                f.result()


@cli.command(help='Calculate database size')
def db_size():
    db_name = settings.DATABASE_URL.rsplit('/', 1)[1]
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT pg_database_size(%s)', (db_name,))
            db_size = cur.fetchone()
            table_sizes = {}
            for table in ['weather']:
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
    with _time('MOSMIX Re-parse'):
        tasks.parse(url=MOSMIX_URL, export=True)


@cli.command(help='Query records from database')
def query_weather():
    # Generate 50 random locations within Germany's bounding box. Locations
    # will be the same across different runs since we hard-code the PRNG seed.
    random.seed(0)
    locations = [
        (random.uniform(47.30, 54.98), random.uniform(5.99, 15.02))
        for _ in range(50)]
    date = datetime.date(2020, 2, 14)
    last_date = datetime.date(2020, 2, 21)
    with _time('50 one-day queries, sequential ', precision=2):
        for lat, lon in locations:
            query.weather(lat, lon, date)
    with _time('50 one-day queries, parallel   ', precision=2):
        with ThreadPoolExecutor(max_workers=len(locations)) as executor:
            for lat, lon in locations:
                executor.submit(query.weather, lat, lon, date)
    with _time('50 one-week queries, sequential', precision=2):
        for lat, lon in locations:
            query.weather(lat, lon, date, last_date)
    with _time('50 one-week queries, parallel  ', precision=2):
        with ThreadPoolExecutor(max_workers=len(locations)) as executor:
            for lat, lon in locations:
                executor.submit(query.weather, lat, lon, date, last_date)


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli()
