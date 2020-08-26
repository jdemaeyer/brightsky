import datetime
import logging
import threading
import time
import os
from contextlib import suppress
from functools import lru_cache

import coloredlogs
import dateutil.parser
from astral import Observer
from astral.sun import daylight
from dateutil.tz import tzlocal, tzutc
from parsel import Selector

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
    resp = requests.get(url, headers={'User-Agent': USER_AGENT})
    resp.raise_for_status()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(resp.content)
    last_modified = dateutil.parser.parse(
        resp.headers['Last-Modified']).timestamp()
    os.utime(path, (last_modified, last_modified))
    return path


def cache_path(url):
    dirname = os.path.join(os.getcwd(), '.cache/brightsky')
    filename = os.path.basename(url)
    return os.path.join(dirname, filename)


def dwd_fingerprint(path):
    """Return file attributes in same format as DWD server index pages"""
    last_modified = datetime.datetime.fromtimestamp(
        os.path.getmtime(path)
    ).replace(
        second=0,
        microsecond=0,
        tzinfo=tzlocal()
    ).astimezone(tzutc())
    return {
        'last_modified': last_modified,
        'file_size': os.path.getsize(path),
    }


def parse_date(date_str):
    d = dateutil.parser.parse(date_str)
    return d


@lru_cache
def sunrise_sunset(lat, lon, date):
    return daylight(Observer(lat, lon), date)


class StationIDConverter:

    STATION_LIST_URL = (
        'https://www.dwd.de/DE/leistungen/klimadatendeutschland/statliste/'
        'statlex_html.html?view=nasPublication')
    STATION_TYPES = ['SY', 'MN']
    UPDATE_INTERVAL = 86400

    update_lock = threading.Lock()

    def __init__(self):
        self.last_update = 0
        self.dwd_to_wmo = {}
        self.wmo_to_dwd = {}

    def update(self, force=False):
        with self.update_lock:
            is_recent = time.time() - self.last_update < self.UPDATE_INTERVAL
            if not force and is_recent:
                return
            logger.info("Updating station ID maps")
            resp = requests.get(
                self.STATION_LIST_URL, headers={'User-Agent': USER_AGENT})
            resp.raise_for_status()
            self.parse_station_list(resp.text)

    def parse_station_list(self, html):
        sel = Selector(html)
        station_rows = []
        for station_type in self.STATION_TYPES:
            station_rows.extend(sel.xpath(
                f'//tr[td[3][text() = "{station_type}"]]'))
        assert station_rows, "No synoptic stations"
        self.dwd_to_wmo.clear()
        self.wmo_to_dwd.clear()
        for row in station_rows:
            values = row.css('td::text').extract()
            dwd_id = values[1].zfill(5)
            wmo_id = values[3]
            self.dwd_to_wmo[dwd_id] = wmo_id
            self.wmo_to_dwd[wmo_id] = dwd_id
        self.last_update = time.time()
        logger.info("Parsed %d station ID mappings", len(station_rows))

    def convert_to_wmo(self, dwd_id):
        self.update()
        return self.dwd_to_wmo.get(dwd_id)

    def convert_to_dwd(self, wmo_id):
        self.update()
        return self.wmo_to_dwd.get(wmo_id)


_converter = StationIDConverter()
dwd_id_to_wmo = _converter.convert_to_wmo
wmo_id_to_dwd = _converter.convert_to_dwd
