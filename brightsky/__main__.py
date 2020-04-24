import os

from brightsky.cli import cli
from brightsky.utils import configure_logging, load_dotenv


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(os.getenv('SENTRY_DSN'))
    cli(prog_name='python -m brightsky')
