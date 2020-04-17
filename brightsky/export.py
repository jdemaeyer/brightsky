import logging
from threading import Lock

from psycopg2 import sql
from psycopg2.extras import execute_values

from brightsky.db import get_connection


logger = logging.getLogger(__name__)


class DBExporter:

    # The ON CONFLICT clause does not actually change anything, but it ensures
    # that the row is returned.
    UPDATE_SOURCES_STMT = """
        INSERT INTO sources (station_id, observation_type, location, height)
        VALUES %s
        ON CONFLICT
            ON CONSTRAINT weather_source_key DO UPDATE SET
                station_id = sources.station_id
        RETURNING id;
    """
    UPDATE_SOURCES_CLEANUP = """
        SELECT setval('sources_id_seq', (SELECT max(id) FROM sources));
    """
    UPDATE_WEATHER_STMT = sql.SQL("""
        INSERT INTO weather (timestamp, source_id, {fields})
        VALUES %s
        ON CONFLICT
            ON CONSTRAINT weather_key DO UPDATE SET
                {conflict_updates};
    """)
    ELEMENT_FIELDS = [
        'precipitation', 'pressure_msl', 'sunshine', 'temperature',
        'wind_direction', 'wind_speed']
    SOURCE_FIELDS = ['station_id', 'observation_type', 'lat', 'lon', 'height']

    sources_update_lock = Lock()

    def export(self, records, fingerprint=None):
        records = list(records)
        sources = self.prepare_sources(records)
        with get_connection() as conn:
            source_map = self.update_sources(conn, sources)
            self.update_weather(conn, source_map, records)
            if fingerprint:
                self.update_parsed_files(conn, fingerprint)

    def prepare_sources(self, records):
        sources = {}
        for r in records:
            r['source'] = tuple(r[field] for field in self.SOURCE_FIELDS)
            sources[r['source']] = {
                field: r[field] for field in self.SOURCE_FIELDS}
        return sources

    def update_sources(self, conn, sources):
        template = (
            '(%(station_id)s, %(observation_type)s, ST_MakePoint(%(lon)s, '
            '%(lat)s), %(height)s)')
        with self.sources_update_lock:
            with conn.cursor() as cur:
                rows = execute_values(
                    cur, self.UPDATE_SOURCES_STMT, sources.values(), template,
                    fetch=True)
                cur.execute(self.UPDATE_SOURCES_CLEANUP)
                conn.commit()
        return {
            source_key: row['id']
            for row, source_key in zip(rows, sources)
        }

    def update_weather(self, conn, source_map, records):
        for r in records:
            r['source_id'] = source_map[r['source']]
        for fields, records in self.make_batches(records).items():
            logger.info(
                "Exporting %d records with fields %s",
                len(records), tuple(fields))
            stmt = self.UPDATE_WEATHER_STMT.format(
                fields=sql.SQL(', ').join(
                    sql.Identifier(f) for f in fields),
                conflict_updates=sql.SQL(', ').join(
                    sql.SQL('{field} = EXCLUDED.{field}').format(
                        field=sql.Identifier(f))
                    for f in fields),
            )
            template = sql.SQL(
                "(%(timestamp)s, %(source_id)s, {values})"
            ).format(
                values=sql.SQL(', ').join(
                    sql.Placeholder(f) for f in fields),
            )
            with conn.cursor() as cur:
                execute_values(cur, stmt, records, template, page_size=1000)

    def make_batches(self, records):
        batches = {}
        for record in records:
            fields = frozenset(f for f in self.ELEMENT_FIELDS if f in record)
            assert fields, "Got record without element fields"
            batch = batches.setdefault(fields, [])
            batch.append(record)
        return batches

    def update_parsed_files(self, conn, fingerprint):
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
