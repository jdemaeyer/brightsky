import datetime

from brightsky.db import get_connection


def weather(lat, lon, date, last_date=None, max_dist=50000):
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    with get_connection() as conn:
        with conn.cursor() as cur:
            params = {
                'location': (lon, lat),
                'date': date,
                'last_date': last_date,
                'max_dist': max_dist,
            }
            cur.execute(
                """
                SELECT DISTINCT ON (timestamp)
                    weather.*,
                    sources.observation_type,
                    sources.station_id,
                    ST_Y(location::geometry) AS lat,
                    ST_X(location::geometry) AS lon,
                    sources.height,
                    ST_Distance(
                        location, ST_MakePoint%(location)s::geography
                    ) AS distance
                FROM weather
                JOIN sources ON sources.id = weather.source_id
                WHERE
                    ST_Distance(
                        location, ST_MakePoint%(location)s::geography
                    ) < %(max_dist)s AND
                    timestamp BETWEEN %(date)s AND %(last_date)s
                ORDER BY timestamp, observation_type, distance
                """,
                params)
            return cur.fetchall()


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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
