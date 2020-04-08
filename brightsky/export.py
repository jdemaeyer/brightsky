import logging

from psycopg2 import sql
from psycopg2.extras import execute_batch

from brightsky.db import get_connection


logger = logging.getLogger(__name__)


class DBExporter:

    UPDATE_STMT = sql.SQL("""
        INSERT INTO weather (
            timestamp, station_id, observation_type, location, height, {fields}
        )
        VALUES (
            %(timestamp)s, %(station_id)s, %(observation_type)s,
            ST_MakePoint%(location)s, %(height)s, {values}
        )
        ON CONFLICT
            ON CONSTRAINT observation_source DO UPDATE SET
                location = ST_MakePoint%(location)s,
                height = %(height)s,
                {conflict_updates};
    """)
    ELEMENT_FIELDS = [
        'precipitation', 'pressure_msl', 'sunshine', 'temperature',
        'wind_direction', 'wind_speed']

    def export(self, records, fingerprint=None):
        records = list(records)
        self.prepare_records(records)
        with get_connection() as conn:
            for fields, records in self.make_batches(records).items():
                logger.info(
                    "Exporting %d records with fields %s",
                    len(records), tuple(fields))
                stmt = self.UPDATE_STMT.format(
                    fields=sql.SQL(', ').join(
                        sql.Identifier(f) for f in fields),
                    values=sql.SQL(', ').join(
                        sql.Placeholder(f) for f in fields),
                    conflict_updates=sql.SQL(', ').join(
                        sql.SQL('{field} = {placeholder}').format(
                            field=sql.Identifier(f),
                            placeholder=sql.Placeholder(f))
                        for f in fields),
                )
                with conn.cursor() as cur:
                    execute_batch(cur, stmt, records)
            if fingerprint:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO parsed_files (
                            url, last_modified, file_size, parsed_at
                        )
                        VALUES (
                            %(url)s, %(last_modified)s, %(file_size)s,
                            current_timestamp
                        )
                        ON CONFLICT
                            ON CONSTRAINT parsed_files_pkey DO UPDATE SET
                                last_modified = %(last_modified)s,
                                file_size = %(file_size)s,
                                parsed_at = current_timestamp;
                        """,
                        fingerprint)
            conn.commit()

    def prepare_records(self, records):
        for r in records:
            r['location'] = (r['lon'], r['lat'])

    def make_batches(self, records):
        batches = {}
        for record in records:
            fields = frozenset(f for f in self.ELEMENT_FIELDS if f in record)
            assert fields, "Got record without element fields"
            batch = batches.setdefault(fields, [])
            batch.append(record)
        return batches
