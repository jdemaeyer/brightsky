import logging
import os
from contextlib import suppress
from functools import lru_cache

import coloredlogs
import dateutil.parser
from astral import Observer
from astral.sun import daylight

import requests


logger = logging.getLogger(__name__)


USER_AGENT = 'Bright Sky / https://brightsky.dev/'


def configure_logging():
    log_fmt = '%(asctime)s %(name)s %(levelname)s  %(message)s'
    coloredlogs.install(level=logging.DEBUG, fmt=log_fmt)
    # Disable some third-party noise
    logging.getLogger('huey').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def load_dotenv(path='.env'):
    if not int(os.getenv('BRIGHTSKY_LOAD_DOTENV', 1)):
        return
    with suppress(FileNotFoundError):
        with open(path) as f:
            for line in f:
                if line.strip() and not line.strip().startswith('#'):
                    key, val = line.strip().split('=', 1)
                    os.environ.setdefault(key, val)


def download(url, directory):
    """
    Download a resource from `url` into `directory`, returning its path and
    fingerprint.
    """
    resp = requests.get(url, headers={'User-Agent': USER_AGENT})
    resp.raise_for_status()
    filename = os.path.basename(url)
    path = os.path.join(directory, filename)
    with open(path, 'wb') as f:
        f.write(resp.content)
    fingerprint = {
        'url': url,
        'last_modified': dateutil.parser.parse(resp.headers['Last-Modified']),
        'file_size': int(resp.headers['Content-Length']),
    }
    return path, fingerprint


def parse_date(date_str):
    try:
        return dateutil.parser.isoparse(date_str)
    except ValueError as e:
        # Auto-correct common error of not encoding '+' as '%2b' in URL
        handled_errors = [
            'Inconsistent use of colon separator',
            'Unused components in ISO string',
        ]
        if e.args and e.args[0] in handled_errors and date_str.count(' ') == 1:
            return parse_date(date_str.replace(' ', '+'))
        raise e from None


@lru_cache
def sunrise_sunset(lat, lon, date):
    return daylight(Observer(lat, lon), date)


def daytime(lat, lon, timestamp):
    try:
        sunrise, sunset = sunrise_sunset(lat, lon, timestamp.date())
    except ValueError as e:
        return 'day' if 'above' in e.args[0] else 'night'
    if sunset < sunrise:
        return 'night' if sunset <= timestamp <= sunrise else 'day'
    return 'day' if sunrise <= timestamp <= sunset else 'night'
