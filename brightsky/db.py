import glob
import logging
import os
import re
from contextlib import contextmanager, suppress
from multiprocessing import cpu_count

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.pool import ThreadedConnectionPool

from brightsky.settings import settings


logger = logging.getLogger(__name__)


@contextmanager
def get_connection():
    if not hasattr(get_connection, '_pool'):
        if 'gunicorn' in os.getenv('SERVER_SOFTWARE', ''):
            # gunicorn sync workers are single-threaded
            minconn = 1
        else:
            minconn = 2*cpu_count()+1
        get_connection._pool = ThreadedConnectionPool(
            minconn, minconn, settings.DATABASE_URL, cursor_factory=DictCursor)
    pool = get_connection._pool
    conn = pool.getconn()
    try:
        with conn:
            yield conn
    except psycopg2.InterfaceError:
        logger.warning('Discarding dead connection pool')
        pool.closeall()
        del get_connection._pool
        raise
    finally:
        if not pool.closed:
            pool.putconn(conn)


def fetch(*args, **kwargs):
    for retry in range(5):
        with suppress(psycopg2.InterfaceError):
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(*args, **kwargs)
                    return cur.fetchall()


def migrate():
    logger.info("Migrating database")
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute('SELECT MAX(id) FROM migrations;')
            except psycopg2.errors.UndefinedTable:
                conn.rollback()
                latest_migration = 0
            else:
                latest_migration = cur.fetchone()[0]

            migration_paths = [
                f for f in sorted(glob.glob('migrations/*.sql'))
                if (m := re.match(r'(\d+)_', os.path.basename(f)))
                and int(m.group(1)) > latest_migration
            ]

            for path in migration_paths:
                logger.info("Applying %s", path)
                match = re.match(r'(\d+)_?(.*)\.sql', os.path.basename(path))
                migration_id = int(match.group(1))
                migration_name = match.group(2)
                with open(path) as f:
                    cur.execute(f.read())
                cur.execute(
                    'INSERT INTO migrations (id, name) VALUES (%s, %s);',
                    (migration_id, migration_name))
                conn.commit()
        if migration_paths:
            logger.info("Applied %d migrations", len(migration_paths))
        else:
            logger.info("No new migrations")
