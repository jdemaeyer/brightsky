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
                    *,
                    ST_Y(location::geometry) AS lat,
                    ST_X(location::geometry) AS lon,
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
