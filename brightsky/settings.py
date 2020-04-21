import datetime
import os

from dateutil.tz import tzutc


DATABASE_URL = 'postgres://localhost'
MIN_DATE = datetime.datetime(2010, 1, 1, tzinfo=tzutc())
MAX_DATE = None
REDIS_URL = 'redis://localhost'


def _make_date(date_str):
    return datetime.datetime.fromisoformat(date_str).replace(tzinfo=tzutc())


_SETTING_PARSERS = {
    'MAX_DATE': _make_date,

    datetime.datetime: _make_date,
}


class Settings(dict):
    """A dictionary that makes its keys available as attributes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded = False

    def load(self):
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
