import datetime
import json
import os
import tempfile
from functools import cached_property

import numpy as np
import requests
from dateutil.tz import tzutc
from isal import isal_zlib as zlib
from pyproj import CRS, Transformer
from shapely import MultiPolygon, STRtree, Point

from brightsky.db import fetch
from brightsky.settings import settings
from brightsky.utils import USER_AGENT


def _make_dicts(rows):
    return [dict(row) for row in rows]


def weather(
        date, last_date=None, lat=None, lon=None, dwd_station_id=None,
        wmo_station_id=None, source_id=None, max_dist=50000):
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    if not date.tzinfo:
        date = date.replace(tzinfo=tzutc())
    if not last_date.tzinfo:
        last_date = last_date.replace(tzinfo=tzutc())
    sources_rows = sources(
        lat=lat, lon=lon, dwd_station_id=dwd_station_id, source_id=source_id,
        wmo_station_id=wmo_station_id, max_dist=max_dist,
        observation_types=['historical', 'current', 'forecast'],
        date=date, last_date=last_date)['sources']
    primary_source_ids = {}
    for row in sources_rows:
        primary_source_ids.setdefault(row['observation_type'], row['id'])
    primary_source_ids = list(primary_source_ids.values())
    weather_rows = _weather(date, last_date, primary_source_ids)
    source_ids = [row['id'] for row in sources_rows]
    if len(weather_rows) < int((last_date - date).total_seconds()) // 3600:
        weather_rows = _weather(date, last_date, source_ids)
    _fill_missing_fields(weather_rows, date, last_date, source_ids, True)
    _fill_missing_fields(weather_rows, date, last_date, source_ids, False)
    used_source_ids = {row['source_id'] for row in weather_rows}
    used_source_ids.update(
        source_id
        for row in weather_rows
        for source_id in row.get('fallback_source_ids', {}).values())
    return {
        'weather': weather_rows,
        'sources': [s for s in sources_rows if s['id'] in used_source_ids],
    }


def _weather(date, last_date, source_id, not_null=None, not_null_or=False):
    params = {
        'date': date,
        'last_date': last_date,
        'source_id': source_id,
    }
    where = "timestamp BETWEEN %(date)s AND %(last_date)s"
    order_by = "timestamp"
    if isinstance(source_id, list):
        where += " AND source_id IN %(source_id_tuple)s"
        order_by += ", array_position(%(source_id)s, source_id)"
        params['source_id_tuple'] = tuple(source_id)
    else:
        where += " AND source_id = %(source_id)s"
    if not_null:
        glue = ' OR ' if not_null_or else ' AND '
        constraint = glue.join(f"{x} IS NOT NULL" for x in not_null)
        where += f" AND ({constraint})"
    sql = f"""
        SELECT DISTINCT ON (timestamp) *
        FROM weather
        WHERE {where}
        ORDER BY {order_by}
    """
    return _make_dicts(fetch(sql, params))


IGNORED_MISSING_FIELDS = {
    # Not available in MOSMIX
    'relative_humidity',
    'wind_gust_direction',
    # Not available in recent and historical measurements
    'precipitation_probability',
    'precipitation_probability_6h',
    # Not measured at many stations
    'solar',
}


def _fill_missing_fields(weather_rows, date, last_date, source_ids, partial):
    incomplete_rows = []
    missing_fields = set()
    for row in weather_rows:
        missing_row_fields = set(k for k, v in row.items() if v is None)
        relevant_fields = missing_row_fields.difference(IGNORED_MISSING_FIELDS)
        if relevant_fields:
            incomplete_rows.append((row, missing_row_fields))
            missing_fields.update(relevant_fields)
    if missing_fields:
        min_date = incomplete_rows[0][0]['timestamp']
        max_date = incomplete_rows[-1][0]['timestamp']
        fallback_rows = {
            row['timestamp']: row
            for row in _weather(
                min_date,
                max_date,
                source_ids,
                not_null=missing_fields,
                not_null_or=partial,
            )
        }
        for row, fields in incomplete_rows:
            fallback_row = fallback_rows.get(row['timestamp'])
            if fallback_row:
                row.setdefault('fallback_source_ids', {})
                for f in fields:
                    if fallback_row[f] is None:
                        continue
                    row[f] = fallback_row[f]
                    row['fallback_source_ids'][f] = fallback_row['source_id']


def current_weather(
        lat=None, lon=None, dwd_station_id=None, wmo_station_id=None,
        source_id=None, max_dist=50000, fallback=True):
    sources_rows = sources(
        lat=lat, lon=lon, dwd_station_id=dwd_station_id,
        wmo_station_id=wmo_station_id, source_id=source_id,
        observation_types=['synop'], max_dist=max_dist,
    )['sources']
    source_ids = [row['id'] for row in sources_rows]
    weather = _current_weather(source_ids)
    if not weather:
        raise LookupError(
            "Could not find current weather for your location criteria")
    used_source_ids = [weather['source_id']]
    if fallback:
        missing_fields = [k for k, v in weather.items() if v is None]
        fallback_weather = _current_weather(
            source_ids, not_null=missing_fields)
        if fallback_weather:
            weather.update({k: fallback_weather[k] for k in missing_fields})
            weather['fallback_source_ids'] = {
                field: fallback_weather['source_id']
                for field in missing_fields}
            used_source_ids.append(fallback_weather['source_id'])
    return {
        'weather': weather,
        'sources': [s for s in sources_rows if s['id'] in used_source_ids],
    }


def _current_weather(source_ids, not_null=None):
    params = {
        'source_ids': source_ids,
        'source_ids_tuple': tuple(source_ids),
    }
    where = "source_id IN %(source_ids_tuple)s"
    if not_null:
        where += ''.join(f" AND {element} IS NOT NULL" for element in not_null)
    sql = f"""
        SELECT *
        FROM current_weather
        WHERE {where}
        ORDER BY array_position(%(source_ids)s, source_id)
        LIMIT 1
    """
    rows = _make_dicts(fetch(sql, params))
    if not rows:
        return {}
    return rows[0]


def synop(
        date, last_date=None, dwd_station_id=None, wmo_station_id=None,
        source_id=None):
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    sources_rows = sources(
        dwd_station_id=dwd_station_id, wmo_station_id=wmo_station_id,
        source_id=source_id, observation_types=['synop'])['sources']
    source_ids = [row['id'] for row in sources_rows]
    sql = """
        SELECT *
        FROM synop
        WHERE
            timestamp BETWEEN %(date)s AND %(last_date)s AND
            source_id IN %(source_ids_tuple)s
        ORDER BY timestamp
        """
    params = {
        'date': date,
        'last_date': last_date,
        'source_ids_tuple': tuple(source_ids),
    }
    return {
        'weather': _make_dicts(fetch(sql, params)),
        'sources': _make_dicts(sources_rows),
    }


def radar(
        date, last_date=None, lat=None, lon=None, distance=200000,
        fmt='compressed', bbox=None):
    extra = {}
    if not last_date:
        last_date = date + datetime.timedelta(hours=2)
    if lat is not None and lon is not None:
        x, y = _transformer.to_xy(lat, lon)
        if not -0.5 <= x <= 1099.5 or not -0.5 <= y <= 1199.5:
            raise LookupError("lat/lon lies outside the radar data range")
        center_x = int(round(x))
        center_y = int(round(y))
        pixels = distance // 1000
        bbox = (
            max(center_y - pixels, 0),
            max(center_x - pixels, 0),
            min(center_y + pixels, 1199),
            min(center_x + pixels, 1099),
        )
        extra['bbox'] = bbox
        extra['latlon_position'] = {
            'x': round(x - bbox[1], 3),
            'y': round(y - bbox[0], 3),
        }
    sql = """
        SELECT *
        FROM radar
        WHERE timestamp BETWEEN %(date)s AND %(last_date)s
        ORDER BY timestamp
        """
    params = {
        'date': date,
        'last_date': last_date,
    }
    rows = fetch(sql, params)
    if fmt == 'plain':
        for row in rows:
            row['precipitation_5'] = _load_radar(row['precipitation_5'], bbox)
    elif fmt == 'bytes':
        for row in rows:
            row['precipitation_5'] = memoryview(
                _load_radar(row['precipitation_5'], bbox),
            )
    elif fmt == 'compressed' and bbox:
        for row in rows:
            row['precipitation_5'] = zlib.compress(
                _load_radar(row['precipitation_5'], bbox),
            )
    elif fmt != 'compressed':
        raise ValueError("Unknown format: '%s'" % fmt)
    return {
        'radar': _make_dicts(rows),
        'geometry': _transformer.bbox_to_geometry(bbox),
        **extra,
    }


def _load_radar(raw, bbox, width=1100, height=1200):
    precip = np.frombuffer(
        zlib.decompress(raw),
        dtype='i2',
    ).reshape((height, width))
    if bbox:
        top, left, bottom, right = bbox
        precip = precip[top:bottom+1, left:right+1]
        # Arrays must be C-contiguous for orjson and zlib
        precip = np.ascontiguousarray(precip).reshape(precip.shape)
    return precip


class RadarCoordinatesTransformer:

    PROJ_STR = (
        "+proj=stere +lat_0=90 +lat_ts=60 +lon_0=10 +a=6378137 "
        "+b=6356752.3142451802 +no_defs +x_0=543196.83521776402 "
        "+y_0=3622588.8619310018"
    )

    @cached_property
    def de1200(self):
        return CRS.from_proj4(self.PROJ_STR)

    @cached_property
    def wgs84_to_de1200(self):
        return Transformer.from_crs(4326, self.de1200)

    @cached_property
    def de1200_to_wgs84(self):
        return Transformer.from_crs(self.de1200, 4326)

    def to_xy(self, lat, lon):
        x, y = self.wgs84_to_de1200.transform(lat, lon)
        return round(x) / 1000, -round(y) / 1000

    def to_latlon(self, x, y):
        lat, lon = self.de1200_to_wgs84.transform(x * 1000, -y * 1000)
        return round(lat, 5), round(lon, 5)

    def to_lonlat(self, x, y):
        return tuple(reversed(self.to_latlon(x, y)))

    def bbox_to_geometry(self, bbox):
        if not bbox:
            bbox = (0, 0, 1199, 1099)
        top, left, bottom, right = bbox
        return {
            'type': 'Polygon',
            'coordinates': [
                self.to_lonlat(left - .5, top - .5),
                self.to_lonlat(left - .5, bottom + .5),
                self.to_lonlat(right + .5, bottom + .5),
                self.to_lonlat(right + .5, top - .5),
            ],
        }


_transformer = RadarCoordinatesTransformer()


def alerts(lat=None, lon=None, warn_cell_id=None):
    if lat is not None and lon is not None:
        meta = _warn_cells.find(lat, lon)
    elif warn_cell_id is not None:
        try:
            meta = _warn_cells.get_meta(warn_cell_id)
        except KeyError:
            raise LookupError(
                "Unknown warn_cell_id, please use commune (Gemeinden), not "
                "district (Landkreis) ids"
            )
    else:
        sql = """
            SELECT *
            FROM alerts
            JOIN (
                SELECT alert_id, array_agg(warn_cell_id) as warn_cell_ids
                FROM alert_cells
                GROUP BY alert_id
            ) cells ON alerts.id = cells.alert_id
            ORDER BY severity DESC
        """
        return {'alerts': _make_dicts(fetch(sql))}
    sql = """
        SELECT *
        FROM alerts
        WHERE id IN (
            SELECT alert_id
            FROM alert_cells
            WHERE warn_cell_id = %(warn_cell_id)s
        )
        ORDER BY severity DESC
        """
    params = {
        'warn_cell_id': meta['warn_cell_id'],
    }
    rows = fetch(sql, params)
    return {
        'alerts': _make_dicts(rows),
        'location': meta,
    }


class WarnCellManager:

    CELLS_CACHE_PATH = os.path.join(tempfile.gettempdir(), 'alert_cells.json')

    @cached_property
    def tree(self):
        self.cell_meta = {}
        self.cell_meta_by_id = {}
        for f in self.get_cell_data()['features']:
            polygons = [
                # shell, holes
                (c[0], c[1:])
                for c in f['geometry']['coordinates']
            ]
            p = MultiPolygon(polygons)
            meta = {
                'warn_cell_id': f['properties']['WARNCELLID'],
                'name': f['properties']['NAME'],
                'name_short': f['properties']['KURZNAME'],
                'district': f['properties']['KREIS'],
                'state': f['properties']['BUNDESLAND'],
                'state_short': f['properties']['BL_KUERZEL'],
            }
            self.cell_meta[p] = meta
            self.cell_meta_by_id[meta['warn_cell_id']] = meta
        return STRtree(list(self.cell_meta.keys()))

    def get_cell_data(self):
        path = self.CELLS_CACHE_PATH
        if not os.path.isfile(path):
            resp = requests.get(
                settings.WARN_CELLS_URL,
                headers={'User-Agent': USER_AGENT},
            )
            with open(path, 'wb') as f:
                f.write(resp.content)
        with open(path) as f:
            return json.load(f)

    def find(self, lat, lon):
        p = Point(lon, lat)
        cell = self.tree.geometries[self.tree.nearest(p)]
        if cell.distance(p) > 0.01:
            raise LookupError("Requested position is not covered by the DWD")
        return self.cell_meta[cell]

    def get_meta(self, warn_cell_id):
        # Make sure cells have been parsed
        self.tree
        return self.cell_meta_by_id[warn_cell_id]


_warn_cells = WarnCellManager()


def sources(
        lat=None, lon=None, dwd_station_id=None, wmo_station_id=None,
        source_id=None, observation_types=None, max_dist=50000,
        ignore_type=False, date=None, last_date=None):
    select = "*"
    order_by = "observation_type"
    params = {
        'lat': lat,
        'lon': lon,
        'max_dist': max_dist,
        'dwd_station_id': dwd_station_id,
        'wmo_station_id': wmo_station_id,
        'source_id': source_id,
        'observation_types': tuple(observation_types or ()),
        'date': date,
        'last_date': last_date,
    }
    if source_id is not None:
        if isinstance(source_id, list):
            where = "id IN %(source_id_tuple)s"
            order_by = (
                "array_position(%(source_id)s, id), observation_type")
            params['source_id_tuple'] = tuple(source_id)
        else:
            where = "id = %(source_id)s"
    elif dwd_station_id is not None:
        if isinstance(dwd_station_id, list):
            where = "dwd_station_id IN %(dwd_station_id_tuple)s"
            order_by = (
                "array_position(%(dwd_station_id)s, dwd_station_id::text), "
                "observation_type")
            params['dwd_station_id_tuple'] = tuple(dwd_station_id)
        else:
            where = "dwd_station_id = %(dwd_station_id)s"
    elif wmo_station_id is not None:
        if isinstance(wmo_station_id, list):
            where = "wmo_station_id IN %(wmo_station_id_tuple)s"
            order_by = (
                "array_position(%(wmo_station_id)s, wmo_station_id::text), "
                "observation_type")
            params['wmo_station_id_tuple'] = tuple(wmo_station_id)
        else:
            where = "wmo_station_id = %(wmo_station_id)s"
    elif (lat is not None and lon is not None):
        distance = """
            earth_distance(
                ll_to_earth(%(lat)s, %(lon)s),
                ll_to_earth(lat, lon)
            )
        """
        select += f", round({distance}) AS distance"
        where = f"""
            earth_box(
                ll_to_earth(%(lat)s, %(lon)s),
                %(max_dist)s
            ) @> ll_to_earth(lat, lon) AND
            {distance} < %(max_dist)s
        """
        if ignore_type:
            order_by = "distance"
        else:
            order_by += ", distance"
    else:
        raise ValueError(
            "Please supply lat/lon or dwd_station_id or wmo_station_id or "
            "source_id")
    if observation_types:
        where += " AND observation_type IN %(observation_types)s"
    if date is not None:
        where += " AND last_record >= %(date)s"
    if last_date is not None:
        where += " AND first_record <= %(last_date)s"
    sql = f"""
        SELECT {select}
        FROM sources
        WHERE {where}
        ORDER BY {order_by}
        """
    rows = fetch(sql, params)
    if not rows:
        raise LookupError("No sources match your criteria")
    return {'sources': _make_dicts(rows)}
