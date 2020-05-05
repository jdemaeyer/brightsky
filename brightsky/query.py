import datetime

from brightsky.db import fetch


def weather(
        date, last_date=None, lat=None, lon=None, station_id=None,
        source_id=None, max_dist=50000):
    select = """
        weather.*,
        sources.observation_type,
        sources.station_id,
        ST_Y(sources.location::geometry) AS lat,
        ST_X(sources.location::geometry) AS lon,
        sources.height
    """
    where = "timestamp BETWEEN %(date)s AND %(last_date)s"
    order_by = "timestamp, observation_type"
    if source_id is not None:
        where += " AND source_id = %(source_id)s"
    elif station_id is not None:
        where += " AND sources.station_id = %(station_id)s"
    elif (lat is not None and lon is not None):
        select += """,
            ST_Distance(
                location, ST_MakePoint%(location)s::geography
            ) AS distance
            """
        where += """ AND
            ST_Distance(
                location, ST_MakePoint%(location)s::geography
            ) < %(max_dist)s
            """
        order_by += ", distance"
    else:
        raise ValueError("Please supply lat/lon or station_id or source_id")
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    sql = f"""
        SELECT DISTINCT ON (timestamp) {select}
        FROM weather
        JOIN sources ON sources.id = weather.source_id
        WHERE {where}
        ORDER BY {order_by}
    """
    params = {
        'date': date,
        'last_date': last_date,
        'location': (lon, lat),
        'max_dist': max_dist,
        'source_id': source_id,
        'station_id': station_id,
    }
    return fetch(sql, params)


def sources(
        lat=None, lon=None, station_id=None, source_id=None, max_dist=50000):
    select = """
        sources.id AS source_id,
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
    return fetch(sql, params)
