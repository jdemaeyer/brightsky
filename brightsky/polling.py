import datetime
import logging
import re

import dateutil.parser
import requests
from dateutil.tz import tzutc
from parsel import Selector

from brightsky.db import fetch
from brightsky.parsers import get_parser


class DWDPoller:

    urls = [
        'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/'
        'all_stations/kml/',
        'https://opendata.dwd.de/weather/weather_reports/synoptic/germany/'
        'json/',
        'https://opendata.dwd.de/weather/weather_reports/poi/',
    ] + [
        'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        f'climate/hourly/{subfolder}/'
        for subfolder in [
            'air_temperature', 'cloudiness', 'dew_point', 'visibility',
            'precipitation', 'pressure', 'sun', 'wind']
    ] + [
        'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        f'climate/10_minutes/extreme_wind/{subfolder}/'
        for subfolder in ['recent', 'historical']
    ]

    @property
    def logger(self):
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(self.__class__.__name__)
        return self._logger

    def poll(self):
        self.logger.info("Polling for updated files")
        parsed_files = {
            row['url']: row
            for row in fetch('SELECT * FROM parsed_files')
        }
        for url in self.urls:
            for file_info in self.poll_url(url):
                if not self.matches_known_fingerprint(parsed_files, file_info):
                    yield file_info

    def poll_url(self, url):
        self.logger.debug("Loading %s", url)
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
                parser_cls = get_parser(link)
                if parser_cls and not parser_cls(url=link_url).should_skip():
                    files.append({
                        'url': link_url,
                        'parser': parser_cls.__name__,
                        'last_modified': last_modified,
                        'file_size': file_size,
                    })
        self.logger.debug(
            "Found %d directories and %d files at %s",
            len(directories), len(files), url)
        yield from files
        for dir_url in directories:
            yield from self.poll_url(dir_url)

    def matches_known_fingerprint(self, parsed_files, file_info):
        parsed_info = parsed_files.get(file_info['url'])
        if not parsed_info:
            return False
        last_modified_diff = abs(
            file_info['last_modified'] - parsed_info['last_modified'])
        # The downloaded file timestamp will sometimes be off from the index
        # page timestamp by one second, presumably because it is delivered from
        # a different server than the one that delivered the index page. If the
        # file was modified at 59 seconds past the minute this can lead to a
        # timestamp that is off by one minute (as seconds are not part of the
        # timestamp). To not re-process these files over and over we allow a
        # one minute time difference from the known fingerprint.
        return (
            file_info['file_size'] == parsed_info['file_size'] and
            last_modified_diff <= datetime.timedelta(minutes=1))
