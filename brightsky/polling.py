import logging
import re

import dateutil.parser
import requests
from dateutil.tz import tzutc
from parsel import Selector

from brightsky.db import get_connection


logger = logging.getLogger(__name__)


class DWDPoller:

    urls = [
        'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/'
        'all_stations/kml/',
        'https://opendata.dwd.de/weather/weather_reports/poi/',
    ] + [
        'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        f'climate/hourly/{subfolder}/'
        for subfolder in [
            'air_temperature', 'precipitation', 'pressure', 'sun', 'wind']
    ]
    parsers = {
        r'MOSMIX_S_LATEST_240\.kmz$': 'MOSMIXParser',
        'stundenwerte_FF_': 'WindObservationsParser',
        'stundenwerte_P0_': 'PressureObservationsParser',
        'stundenwerte_RR_': 'PrecipitationObservationsParser',
        'stundenwerte_SD_': 'SunshineObservationsParser',
        'stundenwerte_TU_': 'TemperatureObservationsParser',
    }

    def poll(self):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM parsed_files')
                parsed_files = {
                    row['url']: (row['last_modified'], row['file_size'])
                    for row in cur.fetchall()
                }
        for url in self.urls:
            for file_info in self.poll_url(url):
                fingerprint = (
                    file_info['last_modified'], file_info['file_size'])
                if parsed_files.get(file_info['url']) != fingerprint:
                    yield file_info

    def poll_url(self, url):
        logger.debug("Loading %s", url)
        resp = requests.get(url)
        resp.raise_for_status()
        return self.parse(url, resp.text)

    def parse(self, url, resp_text):
        sel = Selector(resp_text)
        directories = []
        files = []
        for anchor_sel in sel.css('a'):
            link = anchor_sel.css('::attr(href)').extract_first()
            if link.startswith('.'):
                continue
            link_url = f'{url}{link}'
            if link.endswith('/'):
                directories.append(link_url)
            else:
                fingerprint = anchor_sel.xpath(
                    './following-sibling::text()[1]').extract_first()
                match = re.match(
                    r'\s*(\d+-\w+-\d+ \d+:\d+)\s+(\d+)', fingerprint)
                last_modified = dateutil.parser.parse(
                    match.group(1)).replace(tzinfo=tzutc())
                file_size = int(match.group(2))
                parser = self.get_parser(link)
                if parser:
                    files.append({
                        'url': link_url,
                        'parser': parser,
                        'last_modified': last_modified,
                        'file_size': file_size,
                    })
        logger.info(
            "Found %d directories and %d files at %s",
            len(directories), len(files), url)
        yield from files
        for dir_url in directories:
            yield from self.poll_url(dir_url)

    def get_parser(self, filename):
        for pattern, parser in self.parsers.items():
            if re.match(pattern, filename):
                return parser
