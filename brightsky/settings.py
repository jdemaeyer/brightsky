import datetime
import os

from dateutil.tz import tzutc


DATABASE_URL = 'postgres://localhost'
REDIS_URL = 'redis://localhost'

MIN_DATE = datetime.datetime(2010, 1, 1, tzinfo=tzutc())
MAX_DATE = None


def _make_date(date_str):
    return datetime.datetime.fromisoformat(date_str).replace(tzinfo=tzutc())


_SETTING_PARSERS = {
    'MIN_DATE': _make_date,
    'MAX_DATE': _make_date,
}


class Settings(dict):
    """A dictionary that makes its keys available as attributes"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self

    def load(self):
        for k, v in globals().items():
            if k.isupper() and not k.startswith('_'):
                self[k] = v
        for k, v in os.environ.items():
            if k.startswith('BRIGHTSKY_') and k.isupper():
                setting_name = k.split('_', 1)[1]
                setting_parser = _SETTING_PARSERS.get(setting_name)
                if setting_parser:
                    v = setting_parser(v)
                self[setting_name] = v

    def __getattribute__(self, name):
        Settings.__getattribute__ = dict.__getattribute__
        self.load()
        return self.__getattribute__(name)


settings = Settings()
