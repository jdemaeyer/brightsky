import datetime
import os
import tempfile
from dateutil.tz import tzoffset, tzutc

from brightsky.utils import (
    dwd_fingerprint, parse_date, StationIDConverter, sunrise_sunset)


def test_dwd_fingerprint(data_dir):
    content = b'twelve bytes'
    timestamp = 1597757394.7878118
    with tempfile.NamedTemporaryFile() as f:
        f.write(content)
        f.flush()
        os.utime(f.name, (timestamp, timestamp))
        assert dwd_fingerprint(f.name) == {
            'last_modified': datetime.datetime(
                2020, 8, 18, 13, 29, 0, 0, tzinfo=tzutc()),
            'file_size': 12,
        }


def test_parse_date():
    assert parse_date('2020-08-18') == datetime.datetime(2020, 8, 18, 0, 0)
    assert parse_date('2020-08-18 12:34') == datetime.datetime(
        2020, 8, 18, 12, 34)
    assert parse_date('2020-08-18T12:34:56+02:00') == datetime.datetime(
        2020, 8, 18, 12, 34, 56, tzinfo=tzoffset(None, 7200))


def test_station_id_converter(data_dir):
    c = StationIDConverter()
    with open(data_dir / 'station_list.html') as f:
        c.parse_station_list(f.read())
    assert len(c.dwd_to_wmo) == 4
    assert len(c.wmo_to_dwd) == 5
    assert c.convert_to_wmo('00003') == '10501'
    assert c.convert_to_wmo('01766') == '10315'
    assert c.convert_to_dwd('10315') == '01766'
    # Always use the last row for duplicated DWD IDs
    assert c.convert_to_wmo('05745') == 'F263'


def test_sunrise_sunset():
    sunrise, sunset = sunrise_sunset(52, 7.6, datetime.date(2020, 8, 18))
    assert sunrise < sunset
    assert sunrise.utcoffset().total_seconds() == 0
    assert sunset.utcoffset().total_seconds() == 0
