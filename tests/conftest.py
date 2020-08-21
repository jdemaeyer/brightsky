import os
import time
from pathlib import Path
from urllib.parse import urlparse

import attr
import falcon.testing
import psycopg2
import pytest
from psycopg2.extras import execute_values

from brightsky.db import get_connection, migrate


@pytest.fixture(scope='session')
def data_dir():
    return Path(os.path.dirname(__file__)) / 'data'


@pytest.fixture(scope='session')
def _database():
    if not os.getenv('BRIGHTSKY_DATABASE_URL'):
        pytest.skip('See README for running database-based tests.')
    url = urlparse(os.getenv('BRIGHTSKY_DATABASE_URL'))
    postgres_url = f'postgres://{url.netloc}'
    db_name = url.path.lstrip('/')
    assert db_name
    with psycopg2.connect(postgres_url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS {db_name}')
            cur.execute(f'CREATE DATABASE {db_name}')
    migrate()
    yield
    if hasattr(get_connection, '_pool'):
        get_connection._pool.closeall()
    with psycopg2.connect(postgres_url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE {db_name}')


@attr.s
class TestConnection:
    """Wrapper for database connection with some rough convenience functions"""

    conn = attr.ib()

    def insert(self, table, rows):
        with self.cursor() as cur:
            fields = tuple(rows[0])
            field_placeholders = [f'%({field})s' for field in fields]
            execute_values(
                cur,
                f"INSERT INTO {table} ({', '.join(fields)}) VALUES %s",
                rows,
                template=f"({', '.join(field_placeholders)})")
        self.conn.commit()

    def fetch(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        self.conn.commit()
        return rows

    def table(self, name):
        return self.fetch(f'SELECT * FROM {name}')

    def __getattr__(self, name):
        return getattr(self.conn, name)


@pytest.fixture
def db(_database):
    with get_connection() as conn:
        yield TestConnection(conn)
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM parsed_files;
                DELETE FROM synop;
                DELETE FROM weather;
                DELETE FROM sources;
                REFRESH MATERIALIZED VIEW current_weather;
            """)


@pytest.fixture(scope='session')
def api():
    from brightsky.web import app
    return falcon.testing.TestClient(app)


def pytest_configure(config):
    # Dirty mock so we don't download the station list on every test run
    from brightsky.utils import _converter
    # Must contain all stations that we use in test data
    _converter.dwd_to_wmo = {
        'XXX': '01028',
        'YYY': '01049',
        '01766': '10315',
        '04911': '10788',
        '05484': 'M031',
    }
    _converter.wmo_to_dwd = dict(
        reversed(x) for x in _converter.dwd_to_wmo.items())
    _converter.last_update = time.time()
