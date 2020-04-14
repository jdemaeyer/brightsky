import glob
import logging
import os
import re

import psycopg2
from psycopg2.extras import DictCursor

from brightsky.settings import settings


logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(settings.DATABASE_URL, cursor_factory=DictCursor)


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
