import datetime

from brightsky.db import get_connection


def weather(lat, lon, date, last_date=None):
    if not last_date:
        last_date = date + datetime.timedelta(days=1)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    DISTINCT ON (timestamp) *,
                    ST_Y(location::geometry) AS lat,
                    ST_X(location::geometry) AS lon,
                    ST_Distance(location, ST_MakePoint%s) AS distance
                FROM weather
                WHERE
                    ST_Distance(location, ST_MakePoint%s) < 50000 AND
                    timestamp BETWEEN %s AND %s
                ORDER BY timestamp, observation_type, distance
                """,
                ((lon, lat), (lon, lat), date, last_date)
            )
            return cur.fetchall()
