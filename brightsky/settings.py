import datetime
import os

from dateutil.tz import tzutc

from brightsky.utils import load_dotenv


CORS_ALLOW_ALL_ORIGINS = False
CORS_ALLOW_ALL_HEADERS = False
CORS_ALLOWED_ORIGINS = []
CORS_ALLOWED_HEADERS = []
DATABASE_URL = 'postgres://localhost'
ICON_CLOUDY_THRESHOLD = 80
ICON_PARTLY_CLOUDY_THRESHOLD = 25
ICON_RAIN_THRESHOLD = 0.5
ICON_WIND_THRESHOLD = 10.8
IGNORED_CURRENT_OBSERVATIONS_STATIONS = ['K386']
KEEP_DOWNLOADS = False
MIN_DATE = datetime.datetime(2010, 1, 1, tzinfo=tzutc())
MAX_DATE = None
POLLING_CRONTAB_MINUTE = '*'
REDIS_URL = 'redis://localhost'


def _make_bool(bool_str):
    return bool_str == '1'


def _make_date(date_str):
    return datetime.datetime.fromisoformat(date_str).replace(tzinfo=tzutc())


def _make_list(list_str, separator=','):
    if not list_str:
        return []
    return list_str.split(separator)


_SETTING_PARSERS = {
    'MAX_DATE': _make_date,

    bool: _make_bool,
    datetime.datetime: _make_date,
    float: float,
    list: _make_list,
}


class Settings(dict):
    """A dictionary that makes its keys available as attributes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded = False

    def load(self):
        load_dotenv()
        for k, v in globals().items():
            if k.isupper() and not k.startswith('_'):
                self[k] = v
        for k, v in os.environ.items():
            if k.startswith('BRIGHTSKY_') and k.isupper():
                setting_name = k.split('_', 1)[1]
                setting_type = type(self.get(setting_name))
                setting_parser = _SETTING_PARSERS.get(
                    setting_name, _SETTING_PARSERS.get(setting_type))
                if setting_parser:
                    v = setting_parser(v)
                self[setting_name] = v

    def __getattr__(self, name):
        if not self.loaded:
            self.load()
            self.loaded = True
        return self[name]


settings = Settings()
