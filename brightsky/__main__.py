import os

from brightsky import __version__
from brightsky.cli import cli
from brightsky.utils import configure_logging, load_dotenv


def _getenv_float(key):
    x = os.getenv(key)
    return float(x) if x else None


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(
            dsn=os.getenv('SENTRY_DSN'),
            release=__version__,
            traces_sample_rate=_getenv_float('SENTRY_TRACES_SAMPLE_RATE'),
            profiles_sample_rate=_getenv_float('SENTRY_PROFILES_SAMPLE_RATE'),
        )
    cli(prog_name='python -m brightsky')
