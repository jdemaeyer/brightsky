import datetime

from dateutil.tz import tzutc

from brightsky.settings import Settings

from .utils import environ


def test_settings_loads_environment():
    with environ(BRIGHTSKY_TEST='value'):
        assert Settings().TEST == 'value'


def test_settings_parses_environment_bool():
    assert isinstance(Settings().KEEP_DOWNLOADS, bool)
    with environ(BRIGHTSKY_KEEP_DOWNLOADS='0'):
        assert Settings().KEEP_DOWNLOADS is False
    with environ(BRIGHTSKY_KEEP_DOWNLOADS='1'):
        assert Settings().KEEP_DOWNLOADS is True


def test_settings_parses_environment_date():
    expected = datetime.datetime(2000, 1, 2, tzinfo=tzutc())
    assert isinstance(Settings().MIN_DATE, datetime.datetime)
    with environ(BRIGHTSKY_MIN_DATE='2000-01-02'):
        assert Settings().MIN_DATE == expected
    assert Settings().MAX_DATE is None
    with environ(BRIGHTSKY_MAX_DATE='2000-01-02'):
        assert Settings().MAX_DATE == expected


def test_settings_parses_environment_float():
    assert isinstance(Settings().ICON_RAIN_THRESHOLD, float)
    with environ(BRIGHTSKY_ICON_RAIN_THRESHOLD='0'):
        assert Settings().ICON_RAIN_THRESHOLD == float('0')
    with environ(BRIGHTSKY_ICON_RAIN_THRESHOLD='1.5'):
        assert Settings().ICON_RAIN_THRESHOLD == float('1.5')


def test_settings_parses_environment_list():
    assert isinstance(Settings().CORS_ALLOWED_ORIGINS, list)
    with environ(BRIGHTSKY_CORS_ALLOWED_ORIGINS=''):
        assert Settings().CORS_ALLOWED_ORIGINS == []
    with environ(BRIGHTSKY_CORS_ALLOWED_ORIGINS='a'):
        assert Settings().CORS_ALLOWED_ORIGINS == ['a']
    with environ(BRIGHTSKY_CORS_ALLOWED_ORIGINS='a,b,c'):
        assert Settings().CORS_ALLOWED_ORIGINS == ['a', 'b', 'c']
