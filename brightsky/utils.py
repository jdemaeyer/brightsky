import datetime
import logging
import os

import dateutil.parser
from dateutil.tz import tzutc

import requests


logger = logging.getLogger(__name__)


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


def cache_path(url):
    dirname = os.path.join(os.getcwd(), '.brightsky_cache')
    filename = os.path.basename(url)
    return os.path.join(dirname, filename)


def celsius_to_kelvin(temperature):
    if temperature is not None:
        return round(temperature + 273.15, 2)


def kmh_to_ms(speed):
    if speed is not None:
        return round(speed / 3.6, 1)
