import functools
import logging
from threading import Lock

from psycopg2 import sql
from psycopg2.extras import execute_values

from brightsky.db import get_connection


logger = logging.getLogger(__name__)


class DBExporter:

    # The ON CONFLICT clause won't change anything in most cases, but it
    # ensures that the row is always returned (so that we can build the source
    # map).
    UPDATE_SOURCES_STMT = """
        INSERT INTO sources (
            observation_type, lat, lon, height, dwd_station_id, wmo_station_id,
            station_name, first_record, last_record)
        VALUES %s
        ON CONFLICT
            ON CONSTRAINT weather_source_key DO UPDATE SET
                dwd_station_id = EXCLUDED.dwd_station_id,
                wmo_station_id = EXCLUDED.wmo_station_id,
                station_name = EXCLUDED.station_name,
                first_record = LEAST(
                    sources.first_record, EXCLUDED.first_record),
                last_record = GREATEST(
                    sources.last_record, EXCLUDED.last_record)
        RETURNING id;
    """
    UPDATE_SOURCES_CLEANUP = """
        SELECT setval('sources_id_seq', (SELECT max(id) FROM sources));
    """
    WEATHER_TABLE = 'weather'
    UPDATE_WEATHER_STMT = sql.SQL("""
        INSERT INTO {weather_table} (timestamp, source_id, {fields})
        VALUES %s
        ON CONFLICT
            ON CONSTRAINT {constraint} DO UPDATE SET
                {conflict_updates};
    """)
    UPDATE_WEATHER_CONFLICT_UPDATE = '{field} = EXCLUDED.{field}'
    UPDATE_WEATHER_CLEANUP = None
    SOURCE_FIELDS = [
        'observation_type', 'lat', 'lon', 'height', 'dwd_station_id',
        'wmo_station_id', 'station_name']
    ELEMENT_FIELDS = [
        'cloud_cover', 'condition', 'dew_point', 'precipitation',
        'pressure_msl', 'relative_humidity', 'sunshine', 'temperature',
        'visibility', 'wind_direction', 'wind_speed', 'wind_gust_direction',
        'wind_gust_speed']

    sources_update_lock = Lock()

    def export(self, records, fingerprint=None):
        records = self.prepare_records(records)
        sources = self.prepare_sources(records)
        with get_connection() as conn:
            source_map = self.update_sources(conn, sources)
            self.update_weather(conn, source_map, records)
            if fingerprint:
                self.update_parsed_files(conn, fingerprint)

    def prepare_records(self, records):
        return records

    def prepare_sources(self, records):
        sources = {}
        for r in records:
            r['source'] = tuple(r[field] for field in self.SOURCE_FIELDS)
            source = sources.setdefault(
                r['source'],
                {field: r[field] for field in self.SOURCE_FIELDS})
            if 'first_record' in source:
                source['first_record'] = min(
                    source['first_record'], r['timestamp'])
                source['last_record'] = max(
                    source['last_record'], r['timestamp'])
            else:
                source['first_record'] = r['timestamp']
                source['last_record'] = r['timestamp']
        return sources

    def update_sources(self, conn, sources):
        extra_fields = ['first_record', 'last_record']
        fields = ', '.join(
            f'%({field})s' for field in self.SOURCE_FIELDS + extra_fields)
        with self.sources_update_lock:
            with conn.cursor() as cur:
                rows = execute_values(
                    cur, self.UPDATE_SOURCES_STMT, sources.values(),
                    template=f'({fields})', fetch=True)
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
                weather_table=sql.Identifier(self.WEATHER_TABLE),
                constraint=sql.Identifier(f'{self.WEATHER_TABLE}_key'),
                fields=sql.SQL(', ').join(
                    sql.Identifier(f) for f in fields),
                conflict_updates=sql.SQL(', ').join(
                    sql.SQL(self.UPDATE_WEATHER_CONFLICT_UPDATE).format(
                        field=sql.Identifier(f),
                        weather_table=sql.Identifier(self.WEATHER_TABLE))
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
        if self.UPDATE_WEATHER_CLEANUP:
            with conn.cursor() as cur:
                cur.execute(self.UPDATE_WEATHER_CLEANUP)

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


class SYNOPExporter(DBExporter):

    WEATHER_TABLE = 'synop'
    UPDATE_WEATHER_CONFLICT_UPDATE = (
        '{field} = COALESCE(EXCLUDED.{field}, {weather_table}.{field})')
    UPDATE_WEATHER_CLEANUP = (
        'REFRESH MATERIALIZED VIEW CONCURRENTLY current_weather')

    ELEMENT_FIELDS = [
        'cloud_cover', 'condition', 'dew_point', 'precipitation_10',
        'precipitation_30', 'precipitation_60', 'pressure_msl',
        'relative_humidity', 'sunshine_10', 'sunshine_30', 'sunshine_60',
        'temperature', 'visibility', 'wind_direction_10', 'wind_direction_30',
        'wind_direction_60', 'wind_speed_10', 'wind_speed_30', 'wind_speed_60',
        'wind_gust_direction_10', 'wind_gust_direction_30',
        'wind_gust_direction_60', 'wind_gust_speed_10', 'wind_gust_speed_30',
        'wind_gust_speed_60']

    synop_update_lock = Lock()

    def prepare_records(self, records):
        # Merge records for same source and timestamp (otherwise we will run
        # into trouble with our ON CONFLICT DO UPDATE as we cannot touch the
        # same row twice in the same command).
        records_by_key = {}
        for r in records:
            key = (r['timestamp'], r['wmo_station_id'])
            records_by_key.setdefault(key, []).append(r)
        return [
            functools.reduce(self._update_where_none, records)
            for records in records_by_key.values()]

    def _update_where_none(self, base, update):
        for k, v in update.items():
            if not base.get(k):
                base[k] = v
        return base

    def update_weather(self, *args, **kwargs):
        with self.synop_update_lock:
            super().update_weather(*args, **kwargs)
