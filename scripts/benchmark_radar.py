import array
import os
import time
import zlib
from contextlib import contextmanager

import psycopg2

from brightsky.db import fetch, get_connection
from brightsky.utils import configure_logging, load_dotenv
from brightsky.settings import settings
from brightsky.tasks import parse


@contextmanager
def _time(description):
    start = time.time()
    yield
    delta = int(round((time.time() - start) / 50, 3) * 1000)
    description += ':'
    print(f'{description:15s} {delta:5d} ms')


def setup():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('drop table if exists radar;')
            cur.execute(
                """
                CREATE TABLE radar (
                  timestamp        timestamptz NOT NULL,

                  source           varchar(255) NOT NULL,
                  precipitation_5  smallint[1200][1100] CHECK (
                    array_dims(precipitation_5) = '[1:1200][1:1100]' AND
                    0 <= ALL(precipitation_5)
                  ),
                  precipitation_5_raw  bytea NOT NULL,

                  CONSTRAINT radar_key UNIQUE (timestamp)
                );
                """,
            )
            conn.commit()


def reset():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('delete from radar')
            conn.commit()
    _vacuum()
    parse('https://opendata.dwd.de/weather/radar/composite/rv/DE1200_RV2305021315.tar.bz2')  # noqa
    parse('https://opendata.dwd.de/weather/radar/composite/rv/DE1200_RV2305030700.tar.bz2')  # noqa


def _vacuum():
    conn = psycopg2.connect(settings.DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute('VACUUM ANALYZE radar')
    conn.commit()
    conn.close()


def print_size():
    size = fetch(
        "select pg_size_pretty(pg_total_relation_size('radar'));"
    )[0][0]
    print("DB size:        ", size)


def time_full_array():
    with _time('Array full'):
        fetch('select timestamp, source, precipitation_5 from radar')


def time_clip_array():
    with _time('Array clipped'):
        fetch(
            """
            select timestamp, source, precipitation_5[400:600][400:600]
            from radar
            """
        )


def _make_array(buf):
    a = array.array('H')
    a.frombytes(buf)
    return a


def time_full_bytes():
    with _time('Bytes full'):
        rows = fetch(
            'select timestamp, source, precipitation_5_raw from radar',
        )
        for row in rows:
            data = _make_array(zlib.decompress(row['precipitation_5_raw']))
            # data = [
            #     x if x < 4096 else None
            #     for x in data
            # ]
            precip_5 = [  # noqa
                data[i*1100:(i+1)*1100].tolist()
                for i in reversed(range(1200))
            ]


def time_clip_bytes():
    with _time('Bytes clipped'):
        rows = fetch(
            'select timestamp, source, precipitation_5_raw from radar',
        )
        for row in rows:
            # data = array.array('H')
            raw = zlib.decompress(row['precipitation_5_raw'])
            precip_5 = [  # noqa
                _make_array(raw[i*2200+800:i*2200+1200]).tolist()
                for i in range(400, 600)
            ]
            # data = [
            #     x if x < 4096 else None
            #     for x in data
            # ]
            # precip_5 = [
            #     data[i*1100+400:i*1100+600]
            #     for i in range(400, 600)
            # ]


def main():
    setup()
    for level in range(10):
        print('')
        os.environ['ZLIB_COMPRESSION_LEVEL'] = str(level)
        reset()
        print('\nCOMPRESSION LEVEL', level)
        print_size()
        time_full_array()
        time_clip_array()
        time_full_bytes()
        time_clip_bytes()


if __name__ == '__main__':
    configure_logging()
    load_dotenv()
    main()
