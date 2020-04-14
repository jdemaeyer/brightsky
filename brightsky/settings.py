import os


DATABASE_URL = 'postgres://localhost'
REDIS_URL = 'redis://localhost'


_SETTING_PARSERS = {}



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
