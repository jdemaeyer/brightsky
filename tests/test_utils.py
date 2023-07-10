import datetime
from dateutil.tz import tzoffset, tzutc

from brightsky.utils import daytime, parse_date, sunrise_sunset


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


def test_daytime():
    midnight_0 = datetime.datetime(2023, 7, 10, 0, 0, tzinfo=tzutc())
    noon_0 = datetime.datetime(2023, 7, 10, 12, 0, tzinfo=tzutc())
    midnight_10 = datetime.datetime(2023, 7, 9, 14, 0, tzinfo=tzutc())
    noon_10 = datetime.datetime(2023, 7, 10, 2, 0, tzinfo=tzutc())
    # Muenster
    assert daytime(52, 7.6, midnight_0) == 'night'
    assert daytime(52, 7.6, noon_0) == 'day'
    # Sydney
    assert daytime(-33.8, 151, midnight_10) == 'night'
    assert daytime(-33.8, 151, noon_10) == 'day'
