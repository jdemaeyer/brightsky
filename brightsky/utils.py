import datetime
import logging
import os
from contextlib import suppress

import coloredlogs
import dateutil.parser
from dateutil.tz import tzlocal, tzutc

import requests


logger = logging.getLogger(__name__)


def configure_logging():
    log_fmt = '%(asctime)s %(name)s %(levelname)s  %(message)s'
    coloredlogs.install(level=logging.DEBUG, fmt=log_fmt)
    # Disable some third-party noise
    logging.getLogger('huey').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def load_dotenv(path='.env'):
    with suppress(FileNotFoundError):
        with open(path) as f:
            for line in f:
                if line.strip() and not line.strip().startswith('#'):
                    key, val = line.strip().split('=', 1)
                    os.environ.setdefault(key, val)


def download(url, path):
    """
    Download a resource from `url` to `path`, unless the current version
    already lives at `path`.
    """
    if os.path.isfile(path):
        last_modified_local = datetime.datetime.utcfromtimestamp(
            os.path.getmtime(path)).replace(tzinfo=tzutc())
        resp = requests.head(url)
        resp.raise_for_status()
        last_modified_api = dateutil.parser.parse(
            resp.headers['Last-Modified'])
        is_outdated = last_modified_api > last_modified_local
        if not is_outdated:
            logger.debug(
                '%s is already the newest version, skipping download from %s',
                path, url)
            return
    resp = requests.get(url)
    resp.raise_for_status()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(resp.content)
    last_modified = dateutil.parser.parse(
        resp.headers['Last-Modified']).timestamp()
    os.utime(path, (last_modified, last_modified))


def cache_path(url):
    dirname = os.path.join(os.getcwd(), '.brightsky_cache')
    filename = os.path.basename(url)
    return os.path.join(dirname, filename)


def dwd_fingerprint(path):
    """Return file attributes in same format as DWD server index pages"""
    last_modified = datetime.datetime.fromtimestamp(
        os.path.getmtime(path)
    ).replace(
        second=0,
        tzinfo=tzlocal()
    ).astimezone(tzutc())
    return {
        'last_modified': last_modified,
        'file_size': os.path.getsize(path),
    }


def celsius_to_kelvin(temperature):
    if temperature is not None:
        return round(temperature + 273.15, 2)


def kmh_to_ms(speed):
    if speed is not None:
        return round(speed / 3.6, 1)
