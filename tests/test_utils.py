import datetime
from dateutil.tz import tzoffset

from brightsky.utils import parse_date, sunrise_sunset


def test_parse_date():
    assert parse_date('2020-08-18') == datetime.datetime(2020, 8, 18, 0, 0)
    assert parse_date('2020-08-18 12:34') == datetime.datetime(
        2020, 8, 18, 12, 34)
    assert parse_date('2020-08-18T12:34:56+02:00') == datetime.datetime(
        2020, 8, 18, 12, 34, 56, tzinfo=tzoffset(None, 7200))
    assert parse_date('2020-08-18T12:34:56 02:00') == datetime.datetime(
        2020, 8, 18, 12, 34, 56, tzinfo=tzoffset(None, 7200))


def test_sunrise_sunset():
    sunrise, sunset = sunrise_sunset(52, 7.6, datetime.date(2020, 8, 18))
    assert sunrise < sunset
    assert sunrise.utcoffset().total_seconds() == 0
    assert sunset.utcoffset().total_seconds() == 0
