import datetime

from dateutil.tz import tzutc

from brightsky.settings import Settings

from .utils import environ


def test_settings_loads_environment():
    with environ(BRIGHTSKY_TEST='value'):
        assert Settings().TEST == 'value'


def test_settings_parses_environment_date():
    expected = datetime.datetime(2000, 1, 2, tzinfo=tzutc())
    assert isinstance(Settings().MIN_DATE, datetime.datetime)
    with environ(BRIGHTSKY_MIN_DATE='2000-01-02'):
        assert Settings().MIN_DATE == expected
    assert Settings().MAX_DATE is None
    with environ(BRIGHTSKY_MAX_DATE='2000-01-02'):
        assert Settings().MAX_DATE == expected
