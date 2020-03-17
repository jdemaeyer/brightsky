import csv
import logging
import os
import re
import zipfile

import dateutil.parser
from parsel import Selector

from brightsky.utils import download


logger = logging.getLogger(__name__)


class MOSMIXParser:

    URL = (
        'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/'
        'all_stations/kml/MOSMIX_S_LATEST_240.kmz')

    ELEMENTS = {
        'TTT': 'temperature',
        'DD': 'wind_direction',
        'FF': 'wind_speed',
        'RR1c': 'precipitation',
        'SunD1': 'sunshine',
        'PPPP': 'pressure',
    }

    def __init__(self, path=None):
        self.path = path
        if self.path is None:
            filename = os.path.basename(self.URL)
            dirname = os.path.join(os.getcwd(), '.brightsky_cache')
            self.path = os.path.join(dirname, filename)

    def download(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        download(self.URL, self.path)

    def get_selector(self):
        with zipfile.ZipFile(self.path) as zf:
            infolist = zf.infolist()
            assert len(infolist) == 1, f'Unexpected zip content in {self.path}'
            with zf.open(infolist[0]) as f:
                sel = Selector(f.read().decode('latin1'), type='xml')
        sel.remove_namespaces()
        return sel

    def parse(self):
        sel = self.get_selector()
        timestamps = self.parse_timestamps(sel)
        logger.debug('Got %d timestamps', len(timestamps))
        station_selectors = sel.css('Placemark')
        for i, station_sel in enumerate(station_selectors):
            logger.debug(
                'Parsing station %d / %d', i+1, len(station_selectors))
            yield from self.parse_station(station_sel, timestamps)

    def parse_timestamps(self, sel):
        return [
            dateutil.parser.parse(ts)
            for ts in sel.css('ForecastTimeSteps > TimeStep::text').extract()]

    def parse_station(self, station_sel, timestamps):
        station_id = station_sel.css('name::text').extract_first()
        records = {
            'timestamp': timestamps,
            'station_id': [station_id] * len(timestamps),
        }
        for element, column in self.ELEMENTS.items():
            values_str = station_sel.css(
                f'Forecast[elementName="{element}"] value::text'
            ).extract_first()
            records[column] = [
                None if row[0] == '-' else float(row[0])
                for row in csv.reader(
                    re.sub(r'\s+', '\n', values_str.strip()).splitlines())
            ]
            assert len(records[column]) == len(timestamps)
        # Turn dict of lists into list of dicts
        return (dict(zip(records, l)) for l in zip(*records.values()))
