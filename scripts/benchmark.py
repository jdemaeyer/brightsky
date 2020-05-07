#!/usr/bin/env python

import datetime
import logging
import random
import time
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
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
            finished_futures, _ = wait(futures, return_when=FIRST_EXCEPTION)
            for f in finished_futures:
                # Re-raise any occured exceptions
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
            for table in ['weather', 'sources']:
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
                SELECT station_id, id
                FROM sources
                WHERE observation_type = %s
                """,
                ('recent',))
            rows = random.choices(cur.fetchall(), k=100)
            station_kwargs = [
                {'station_id': row['station_id']} for row in rows]
            source_kwargs = [{'source_id': row['id']} for row in rows]
    date = datetime.date(2020, 2, 14)
    last_date = datetime.date(2020, 2, 21)

    def _test_with_kwargs(desc, kwargs_list, **extra_kwargs):
        with _time('  100  one-day queries, sequential', precision=2):
            for kwargs in kwargs_list:
                query.weather(date, **kwargs, **extra_kwargs)
        with _time('  100  one-day queries, parallel:  ', precision=2):
            with ThreadPoolExecutor(max_workers=2*cpu_count()+1) as executor:
                for kwargs in kwargs_list:
                    executor.submit(
                        query.weather, date, **kwargs, **extra_kwargs)
        with _time('  100 one-week queries, sequential', precision=2):
            for kwargs in kwargs_list:
                query.weather(
                    date, last_date=last_date, **kwargs, **extra_kwargs)
        with _time('  100 one-week queries, parallel:  ', precision=2):
            with ThreadPoolExecutor(max_workers=2*cpu_count()+1) as executor:
                for kwargs in kwargs_list:
                    executor.submit(
                        query.weather,
                        date,
                        last_date=last_date, **kwargs, **extra_kwargs)

    click.echo('By lat/lon:')
    _test_with_kwargs('by lat/lon,', location_kwargs)
    click.echo('\nBy lat/lon, no fallback:')
    _test_with_kwargs(
        'by lat/lon, no fallback,', location_kwargs, fallback=False)
    click.echo('\nBy station:')
    _test_with_kwargs('by station,', station_kwargs)
    click.echo('\nBy source:')
    _test_with_kwargs('by source, ', source_kwargs)


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli()
