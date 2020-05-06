import datetime

from brightsky.db import fetch


def _make_dicts(rows):
    return [dict(row) for row in rows]


def weather(
        date, last_date=None, lat=None, lon=None, station_id=None,
        source_id=None, max_dist=50000):
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    if source_id is not None:
        return {'weather': _weather(date, last_date, source_id)}
    else:
        sources_rows = sources(
            lat=lat, lon=lon, station_id=station_id, max_dist=max_dist
        )['sources']
        weather_rows = _weather(
            date, last_date, [row['id'] for row in sources_rows])
        used_source_ids = {row['source_id'] for row in weather_rows}
        return {
            'weather': weather_rows,
            'sources': [s for s in sources_rows if s['id'] in used_source_ids],
        }


def _weather(date, last_date, source_id):
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
    sql = f"""
        SELECT DISTINCT ON (timestamp) *
        FROM weather
        WHERE {where}
        ORDER BY {order_by}
    """
    return _make_dicts(fetch(sql, params))


def sources(
        lat=None, lon=None, station_id=None, source_id=None, max_dist=50000):
    select = """
        id,
        station_id,
        observation_type,
        ST_Y(location::geometry) AS lat,
        ST_X(location::geometry) AS lon,
        height
    """
    order_by = "observation_type"
    if source_id is not None:
        where = "id = %(source_id)s"
    elif station_id is not None:
        where = "station_id = %(station_id)s"
    elif (lat is not None and lon is not None):
        select += """,
            ST_Distance(
                location, ST_MakePoint%(location)s::geography
            ) AS distance
        """
        where = """
            ST_Distance(
                location, ST_MakePoint%(location)s::geography
            ) < %(max_dist)s
        """
        order_by += ", distance"
    else:
        raise ValueError("Please supply lat/lon or station_id or source_id")
    sql = f"""
        SELECT {select}
        FROM sources
        WHERE {where}
        ORDER BY {order_by}
        """
    params = {
        'location': (lon, lat),
        'max_dist': max_dist,
        'station_id': station_id,
        'source_id': source_id,
    }
    return {'sources': _make_dicts(fetch(sql, params))}
