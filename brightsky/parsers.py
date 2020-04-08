import csv
import datetime
import io
import logging
import os
import re
import zipfile

import dateutil.parser
from dateutil.tz import tzutc
from parsel import Selector

from brightsky.db import get_connection
from brightsky.utils import cache_path, celsius_to_kelvin, download, kmh_to_ms


class Parser:

    DEFAULT_URL = None

    @property
    def logger(self):
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(self.__class__.__name__)
        return self._logger

    def __init__(self, path=None, url=None):
        self.url = url or self.DEFAULT_URL
        self.path = path
        if not self.path and self.url:
            self.path = cache_path(self.url)

    def download(self):
        self.logger.info('Downloading "%s" to "%s"', self.url, self.path)
        download(self.url, self.path)


class MOSMIXParser(Parser):

    DEFAULT_URL = (
        'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/'
        'all_stations/kml/MOSMIX_S_LATEST_240.kmz')

    ELEMENTS = {
        'TTT': 'temperature',
        'DD': 'wind_direction',
        'FF': 'wind_speed',
        'RR1c': 'precipitation',
        'SunD1': 'sunshine',
        'PPPP': 'pressure_msl',
    }

    def parse(self):
        self.logger.info("Parsing %s", self.path)
        sel = self.get_selector()
        timestamps = self.parse_timestamps(sel)
        source = self.parse_source(sel)
        self.logger.debug(
            'Got %d timestamps for source %s', len(timestamps), source)
        station_selectors = sel.css('Placemark')
        for i, station_sel in enumerate(station_selectors):
            self.logger.debug(
                'Parsing station %d / %d', i+1, len(station_selectors))
            records = self.parse_station(station_sel, timestamps, source)
            yield from self.sanitize_records(records)

    def get_selector(self):
        with zipfile.ZipFile(self.path) as zf:
            infolist = zf.infolist()
            assert len(infolist) == 1, f'Unexpected zip content in {self.path}'
            with zf.open(infolist[0]) as f:
                sel = Selector(f.read().decode('latin1'), type='xml')
        sel.remove_namespaces()
        return sel

    def parse_timestamps(self, sel):
        return [
            dateutil.parser.parse(ts)
            for ts in sel.css('ForecastTimeSteps > TimeStep::text').extract()]

    def parse_source(self, sel):
        return ':'.join(sel.css('ProductID::text, IssueTime::text').extract())

    def parse_station(self, station_sel, timestamps, source):
        station_id = station_sel.css('name::text').extract_first()
        lat, lon, height = station_sel.css(
            'coordinates::text').extract_first().split(',')
        records = {'timestamp': timestamps}
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
        base_record = {
            'observation_type': 'forecast',
            'source': source,
            'station_id': station_id,
            'lat': float(lat),
            'lon': float(lon),
            'height': float(height),
        }
        # Turn dict of lists into list of dicts
        return (
            {**base_record, **dict(zip(records, row))}
            for row in zip(*records.values())
        )

    def sanitize_records(self, records):
        for r in records:
            if r['precipitation'] and r['precipitation'] < 0:
                self.logger.warning(
                    "Ignoring negative precipitation value: %s", r)
                r['precipitation'] = None
            yield r


class CurrentObservationsParser(Parser):

    ELEMENTS = {
        'dry_bulb_temperature_at_2_meter_above_ground': 'temperature',
        'mean_wind_direction_during_last_10 min_at_10_meters_above_ground': (
            'wind_direction'),
        'mean_wind_speed_during last_10_min_at_10_meters_above_ground': (
            'wind_speed'),
        'precipitation_amount_last_hour': 'precipitation',
        'pressure_reduced_to_mean_sea_level': 'pressure_msl',
        'total_time_of_sunshine_during_last_hour': 'sunshine',
    }
    DATE_COLUMN = 'surface observations'
    HOUR_COLUMN = 'Parameter description'

    CONVERSION_FACTORS = {
        # hPa to Pa
        'pressure_msl': 100,
        # minutes to seconds
        'sunshine': 60,
    }

    def parse(self, lat=None, lon=None, height=None):
        with open(self.path) as f:
            reader = csv.DictReader(f, delimiter=';')
            station_id = next(reader)[self.DATE_COLUMN].rstrip('_')
            if lat is None or lon is None or height is None:
                lat, lon, height = self.load_location(station_id)
            # Skip row with German header titles
            next(reader)
            for row in reader:
                yield {
                    'observation_type': 'current',
                    'station_id': station_id,
                    'lat': lat,
                    'lon': lon,
                    'height': height,
                    **self.parse_row(row)
                }

    def parse_row(self, row):
        record = {
            element: (
                None
                if row[column] == '---'
                else float(row[column].replace(',', '.')))
            for column, element in self.ELEMENTS.items()
        }
        record['timestamp'] = datetime.datetime.strptime(
            f'{row[self.DATE_COLUMN]} {row[self.HOUR_COLUMN]}',
            '%d.%m.%y %H:%M'
        ).replace(tzinfo=tzutc())
        self.convert_units(record)
        return record

    def convert_units(self, record):
        for element, factor in self.CONVERSION_FACTORS.items():
            if record[element] is not None:
                record[element] *= factor
        record['temperature'] = celsius_to_kelvin(record['temperature'])
        record['wind_speed'] = kmh_to_ms(record['wind_speed'])

    def load_location(self, station_id):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        ST_Y(location::geometry) AS lat,
                        ST_X(location::geometry) AS lon,
                        height
                    FROM weather
                    WHERE observation_type = %s AND station_id = %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    ('forecast', station_id),
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError(
                        f'Unable to find location for station {station_id}')
                return row


class ObservationsParser(Parser):

    elements = {}
    conversion_factors = {}

    def parse(self):
        with zipfile.ZipFile(self.path) as zf:
            station_id = self.parse_station_id(zf)
            observation_type = self.parse_observation_type()
            lat_lon_history = self.parse_lat_lon_history(zf, station_id)
            for record in self.parse_records(zf, lat_lon_history):
                yield {
                    'observation_type': observation_type,
                    'station_id': station_id,
                    **record
                }

    def parse_station_id(self, zf):
        for filename in zf.namelist():
            if (m := re.match(r'Metadaten_Geographie_(\d+)\.txt', filename)):
                return m.group(1)
        raise ValueError(f"Unable to parse station ID for {self.path}")

    def parse_observation_type(self):
        filename = os.path.basename(self.path)
        if filename.endswith('_akt.zip'):
            return 'recent'
        elif filename.endswith('_hist.zip'):
            return 'historical'
        raise ValueError(
            f'Unable to determine observation type from path "{self.path}"')

    def parse_lat_lon_history(self, zf, station_id):
        with zf.open(f'Metadaten_Geographie_{station_id}.txt') as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f, encoding='latin1'),
                delimiter=';')
            history = {}
            for row in reader:
                date_from = datetime.datetime.strptime(
                    row['von_datum'].strip(), '%Y%m%d'
                ).replace(tzinfo=tzutc())
                history[date_from] = (
                    float(row['Geogr.Laenge']),
                    float(row['Geogr.Breite']),
                    float(row['Stationshoehe']))
            return history

    def parse_records(self, zf, lat_lon_history):
        product_filenames = [
            fn for fn in zf.namelist() if fn.startswith('produkt_')]
        assert len(product_filenames) == 1, "Unexpected product count"
        filename = product_filenames[0]
        with zf.open(filename) as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f, encoding='latin1'),
                delimiter=';')
            for row in reader:
                timestamp = datetime.datetime.strptime(
                    row['MESS_DATUM'], '%Y%m%d%H').replace(tzinfo=tzutc())
                for date, lat_lon_height in lat_lon_history.items():
                    if date > timestamp:
                        break
                    lat, lon, height = lat_lon_height
                yield {
                    'source': f'Observations:Recent:{filename}',
                    'lat': lat,
                    'lon': lon,
                    'height': height,
                    'timestamp': timestamp,
                    **self.parse_elements(row),
                }

    def parse_elements(self, row):
        elements = {
            element: (
                float(row[element_key])
                if row[element_key].strip() != '-999'
                else None)
            for element, element_key in self.elements.items()
        }
        for element, factor in self.conversion_factors.items():
            elements[element] *= factor
            elements[element] = round(elements[element], 2)
        return elements


class TemperatureObservationsParser(ObservationsParser):

    elements = {
        'temperature': 'TT_TU',
    }

    def parse_elements(self, row):
        elements = super().parse_elements(row)
        elements['temperature'] = celsius_to_kelvin(elements['temperature'])
        return elements


class PrecipitationObservationsParser(ObservationsParser):

    elements = {
        'precipitation': '  R1',
    }


class WindObservationsParser(ObservationsParser):

    elements = {
        'wind_speed': '   F',
        'wind_direction': '   D',
    }


class SunshineObservationsParser(ObservationsParser):

    elements = {
        'sunshine': 'SD_SO',
    }
    conversion_factors = {
        # Minutes to seconds
        'sunshine': 60,
    }


class PressureObservationsParser(ObservationsParser):

    elements = {
        'pressure_msl': '  P0',
    }
    conversion_factors = {
        # hPa to Pa
        'pressure_msl': 100,
    }


def get_parser(filename):
    parsers = {
        r'MOSMIX_S_LATEST_240\.kmz$': MOSMIXParser,
        r'\w{5}-BEOB\.csv$': CurrentObservationsParser,
        'stundenwerte_FF_': WindObservationsParser,
        'stundenwerte_P0_': PressureObservationsParser,
        'stundenwerte_RR_': PrecipitationObservationsParser,
        'stundenwerte_SD_': SunshineObservationsParser,
        'stundenwerte_TU_': TemperatureObservationsParser,
    }
    for pattern, parser in parsers.items():
        if re.match(pattern, filename):
            return parser
