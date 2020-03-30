import logging
import re

import requests
from parsel import Selector


logger = logging.getLogger(__name__)


class DWDPoller:

    urls = [
        'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/'
        'all_stations/kml/',
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
        for url in self.urls:
            yield from self.poll_url(url)

    def poll_url(self, url):
        logger.debug("Loading %s", url)
        resp = requests.get(url)
        resp.raise_for_status()
        return self.parse(url, resp.text)

    def parse(self, url, resp_text):
        sel = Selector(resp_text)
        directories = []
        files = []
        for link in sel.css('a::attr(href)').extract():
            if link.startswith('.'):
                continue
            link_url = f'{url}{link}'
            if link.endswith('/'):
                directories.append(link_url)
            else:
                for pattern, parser in self.parsers.items():
                    if re.match(pattern, link):
                        files.append({
                            'url': link_url,
                            'parser': parser,
                        })
        logger.info(
            "Found %d directories and %d files at %s",
            len(directories), len(files), url)
        yield from files
        for dir_url in directories:
            yield from self.poll_url(dir_url)
