import functools
import logging
from itertools import islice
from threading import Lock

from psycopg2 import sql
from psycopg2.extras import execute_values

from brightsky.db import get_connection


logger = logging.getLogger(__name__)


def batched(it, batch_size):
    it = iter(it)
    while batch := tuple(islice(it, batch_size)):
        yield batch


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
    UPDATE_WEATHER_VALUES_TEMPLATE = '(%(timestamp)s, %(source_id)s, {values})'
    UPDATE_WEATHER_CLEANUP = None
    SOURCE_FIELDS = [
        'observation_type', 'lat', 'lon', 'height', 'dwd_station_id',
        'wmo_station_id', 'station_name']
    ELEMENT_FIELDS = [
        'cloud_cover',
        'condition',
        'dew_point',
        'precipitation',
        'precipitation_probability',
        'precipitation_probability_6h',
        'pressure_msl',
        'relative_humidity',
        'solar',
        'sunshine',
        'temperature',
        'visibility',
        'wind_direction',
        'wind_speed',
        'wind_gust_direction',
        'wind_gust_speed',
    ]

    sources_update_lock = Lock()

    BATCH_SIZE = 10000

    def export(self, records, fingerprint=None):
        with get_connection() as conn:
            for batch in batched(records, self.BATCH_SIZE):
                self.export_batch(conn, batch)
            if fingerprint:
                self.update_parsed_files(conn, fingerprint)
            conn.commit()

    def export_batch(self, conn, batch):
        records = self.prepare_records(batch)
        sources = self.prepare_sources(batch)
        source_map = self.update_sources(conn, sources)
        self.map_source_ids(records, source_map)
        self.update_weather(conn, records)

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

    def map_source_ids(self, records, source_map):
        for r in records:
            r['source_id'] = source_map[r['source']]

    def update_weather(self, conn, records):
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
                self.UPDATE_WEATHER_VALUES_TEMPLATE,
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
        'cloud_cover',
        'condition',
        'dew_point',
        'precipitation_10',
        'precipitation_30',
        'precipitation_60',
        'pressure_msl',
        'relative_humidity',
        'solar_10',
        'solar_30',
        'solar_60',
        'sunshine_10',
        'sunshine_30',
        'sunshine_60',
        'temperature',
        'visibility',
        'wind_direction_10',
        'wind_direction_30',
        'wind_direction_60',
        'wind_speed_10',
        'wind_speed_30',
        'wind_speed_60',
        'wind_gust_direction_10',
        'wind_gust_direction_30',
        'wind_gust_direction_60',
        'wind_gust_speed_10',
        'wind_gust_speed_30',
        'wind_gust_speed_60',
    ]

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


class RADOLANExporter(DBExporter):

    WEATHER_TABLE = 'radar'
    UPDATE_WEATHER_STMT = sql.SQL("""
        INSERT INTO {weather_table} (timestamp, {fields})
        VALUES %s
        ON CONFLICT
            ON CONSTRAINT {constraint} DO UPDATE SET
                {conflict_updates};
    """)
    UPDATE_WEATHER_VALUES_TEMPLATE = '(%(timestamp)s, {values})'
    ELEMENT_FIELDS = [
        'precipitation_5',
        'source',
    ]

    def export_batch(self, conn, batch):
        records = self.prepare_records(batch)
        self.update_weather(conn, records)


class AlertExporter(DBExporter):

    UPDATE_ALERTS_STMT = sql.SQL("""
        INSERT INTO alerts ({fields})
        VALUES %s
        ON CONFLICT
            ON CONSTRAINT alerts_key DO UPDATE SET
                {conflict_updates}
        RETURNING id;
    """)
    UPDATE_ALERTS_CLEANUP = """
        SELECT setval(
            'alerts_id_seq',
            GREATEST(%(max_id)s, (SELECT max(id) FROM alerts))
        );
    """
    ELEMENT_FIELDS = [
        'alert_id',
        'effective',
        'onset',
        'expires',
        'category',
        'response_type',
        'urgency',
        'severity',
        'certainty',
        'event_code',
        'event_en',
        'event_de',
        'headline_en',
        'headline_de',
        'description_en',
        'description_de',
        'instruction_en',
        'instruction_de',
    ]

    def export(self, alerts, fingerprint=None):
        alerts = list(alerts)
        self.prepare_alerts(alerts)
        with get_connection() as conn:
            last_id = self.get_last_alert_id(conn)
            self.clear_outdated_alerts(conn, alerts)
            self.update_alerts(conn, alerts)
            self.reset_alert_id(conn, last_id)
            self.update_alert_cells(conn, alerts)
            if fingerprint:
                self.update_parsed_files(conn, fingerprint)
            conn.commit()

    def prepare_alerts(self, alerts):
        for a in alerts:
            a['alert_id'] = a.pop('id')

    def get_last_alert_id(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(id) FROM alerts")
            return cur.fetchall()[0]['max']

    def clear_outdated_alerts(self, conn, alerts):
        with conn.cursor() as cur:
            cur.execute("SELECT alert_id FROM alerts")
            existing = set(row['alert_id'] for row in cur.fetchall())
            outdated = existing.difference(a['alert_id'] for a in alerts)
            if outdated:
                logger.info("Deleting %d outdated alerts", len(outdated))
                cur.execute(
                    "DELETE FROM alerts WHERE alert_id IN %(outdated)s",
                    {'outdated': tuple(outdated)},
                )

    def update_alerts(self, conn, alerts):
        for fields, alerts in self.make_batches(alerts).items():
            logger.info(
                "Exporting %d alerts with fields %s",
                len(alerts),
                tuple(fields),
            )
            conflict_updates = sql.SQL(', ').join(
                sql.SQL('{field} = EXCLUDED.{field}').format(
                    field=sql.Identifier(f),
                )
                for f in fields
            )
            stmt = self.UPDATE_ALERTS_STMT.format(
                fields=sql.SQL(', ').join(sql.Identifier(f) for f in fields),
                conflict_updates=conflict_updates,
            )
            template = sql.SQL(
                '({values})',
            ).format(
                values=sql.SQL(', ').join(
                    sql.Placeholder(f) for f in fields
                ),
            )
            with conn.cursor() as cur:
                rows = execute_values(
                    cur,
                    stmt,
                    alerts,
                    template,
                    page_size=1000,
                    fetch=True,
                )
            for row, alert in zip(rows, alerts, strict=True):
                alert['id'] = row['id']

    def reset_alert_id(self, conn, max_id):
        if max_id is None:
            return
        with conn.cursor() as cur:
            cur.execute(self.UPDATE_ALERTS_CLEANUP, {'max_id': max_id})

    def update_alert_cells(self, conn, alerts):
        rows = [
            (alert['id'], wcid)
            for alert in alerts
            for wcid in alert['warn_cell_ids']
        ]
        with conn.cursor() as cur:
            cur.execute("DELETE FROM alert_cells")
            execute_values(
                cur,
                "INSERT INTO alert_cells (alert_id, warn_cell_id) VALUES %s",
                rows,
                page_size=1000,
            )
