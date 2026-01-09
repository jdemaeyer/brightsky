import os
import logging

from brightsky import __version__
from brightsky.cli import cli
from brightsky.utils import configure_logging, load_dotenv


def _getenv_float(key):
    """Return the float value of environment variable `key`, or None."""
    x = os.getenv(key)
    if not x:
        return None
    try:
        return float(x.strip())
    except ValueError:
        logging.getLogger(__name__).warning(
            "Invalid float value for %s: %r. Using None instead.", key, x)
        return None


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
