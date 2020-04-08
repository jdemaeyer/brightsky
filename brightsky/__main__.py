import logging
import os
from contextlib import suppress

import coloredlogs

from brightsky.cli import cli


logger = logging.getLogger('brightsky')


def configure_logging():
    log_fmt = '%(asctime)s %(name)s %(levelname)s  %(message)s'
    coloredlogs.install(level=logging.DEBUG, fmt=log_fmt)
    # Disable some third-party noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def load_dotenv(path='.env'):
    with suppress(FileNotFoundError):
        with open(path) as f:
            for line in f:
                if line.strip() and not line.strip().startswith('#'):
                    key, val = line.strip().split('=', 1)
                    os.environ.setdefault(key, val)


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli(prog_name='python -m brightsky')
