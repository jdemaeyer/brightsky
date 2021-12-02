import bz2
import csv
import datetime
import io
import json
import logging
import os
import re
import zipfile
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress

import dateutil.parser
from dateutil.tz import tzutc
from parsel import Selector

from brightsky.db import fetch
from brightsky.export import DBExporter, SYNOPExporter
from brightsky.settings import settings
from brightsky.units import (
    celsius_to_kelvin, current_observations_weather_code_to_condition,
    eighths_to_percent, hpa_to_pa, km_to_m, kmh_to_ms, minutes_to_seconds,
    synop_current_weather_code_to_condition,
    synop_form_of_precipitation_code_to_condition,
    synop_past_weather_code_to_condition)
from brightsky.utils import cache_path, download, dwd_id_to_wmo, wmo_id_to_dwd


class SkipRecord(Exception):
    pass


class Parser:

    DEFAULT_URL = None
    PRIORITY = 10

    exporter = DBExporter

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
        self.downloaded_files = set()

    def download(self):
        self._download(self.url, self.path)

    def _download(self, url, path):
        self.logger.info('Downloading "%s" to "%s"', url, path)
        download_path = download(url, path)
        if download_path:
            self.downloaded_files.add(download_path)

    def should_skip(self):
        return False

    def parse(self):
        raise NotImplementedError

    def cleanup(self):
        if not settings.KEEP_DOWNLOADS:
            for path in self.downloaded_files:
                self.logger.debug("Removing '%s'", path)
                with suppress(FileNotFoundError):
                    os.remove(path)


class MOSMIXParser(Parser):

    DEFAULT_URL = (
        'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/'
        'all_stations/kml/MOSMIX_S_LATEST_240.kmz')
    PRIORITY = 20

    ELEMENTS = {
        'DD': 'wind_direction',
        'FF': 'wind_speed',
        'FX1': 'wind_gust_speed',
        'N': 'cloud_cover',
        'PPPP': 'pressure_msl',
        'RR1c': 'precipitation',
        'SunD1': 'sunshine',
        'Td': 'dew_point',
        'TTT': 'temperature',
        'VV': 'visibility',
        'ww': 'condition',
    }

    def parse(self):
        # Wrap in process as it seems to be leaking memory somewhere in lxml
        with ProcessPoolExecutor() as executor:
            f = executor.submit(MOSMIXParser._get_records, self)
            return f.result()

    @staticmethod
    def _get_records(parser):
        return list(parser._parse())

    def _parse(self):
        self.logger.info("Parsing %s", self.path)
        sel = self.get_selector()
        timestamps = self.parse_timestamps(sel)
        source = self.parse_source(sel)
        self.logger.debug(
            'Got %d timestamps for source %s', len(timestamps), source)
        for station_sel in sel.css('Placemark'):
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
        wmo_station_id = station_sel.css('name::text').extract_first()
        dwd_station_id = wmo_id_to_dwd(wmo_station_id)
        station_name = station_sel.css('description::text').extract_first()
        try:
            lon, lat, height = station_sel.css(
                'coordinates::text').extract_first().split(',')
        except AttributeError:
            self.logger.warning(
                "Ignoring station without coordinates, WMO ID '%s', DWD ID "
                "'%s', name '%s'",
                wmo_station_id, dwd_station_id, station_name)
            return []
        records = {'timestamp': timestamps}
        for element, column in self.ELEMENTS.items():
            values_str = station_sel.css(
                f'Forecast[elementName="{element}"] value::text'
            ).extract_first()
            converter = getattr(self, f'parse_{column}', float)
            records[column] = [
                None if row[0] == '-' else converter(row[0])
                for row in csv.reader(
                    re.sub(r'\s+', '\n', values_str.strip()).splitlines())
            ]
            assert len(records[column]) == len(timestamps)
        base_record = {
            'observation_type': 'forecast',
            'source': source,
            'lat': float(lat),
            'lon': float(lon),
            'height': float(height),
            'dwd_station_id': dwd_station_id,
            'wmo_station_id': wmo_station_id,
            'station_name': station_name,
        }
        # Turn dict of lists into list of dicts
        return (
            {**base_record, **dict(zip(records, row))}
            for row in zip(*records.values())
        )

    def parse_condition(self, value):
        code = int(value.split('.')[0])
        return synop_current_weather_code_to_condition(code)

    def sanitize_records(self, records):
        for r in records:
            if r['precipitation'] and r['precipitation'] < 0:
                self.logger.warning(
                    "Ignoring negative precipitation value: %s", r)
                r['precipitation'] = None
            if r['wind_direction'] and r['wind_direction'] > 360:
                self.logger.warning(
                    "Fixing out-of-bounds wind direction: %s", r)
                r['wind_direction'] -= 360
            yield r


class SYNOPParser(Parser):

    PRIORITY = 30

    exporter = SYNOPExporter

    mandatory_fields = [
        'wmo_station_id', 'station_name', 'lat', 'lon', 'height', 'timestamp']

    elements = {
        'cloudCoverTotal': 'cloud_cover',
        'heightOfStationGroundAboveMeanSeaLevel': 'height',
        'latitude': 'lat',
        'longitude': 'lon',
        'meteorologicalOpticalRange': 'visibility',
        'pressureReducedToMeanSeaLevel': 'pressure_msl',
        'stationOrSiteName': 'station_name',
    }
    height_field = 'heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform'
    height = 2
    height_elements = {
        'airTemperature': 'temperature',
        'dewpointTemperature': 'dew_point',
        'relativeHumidity': 'relative_humidity',
    }
    time_period_field = 'timePeriod'
    time_periods = (-10, -30, -60)
    time_period_elements = {
        'windDirection': 'wind_direction',
        'windSpeed': 'wind_speed',
        'maximumWindGustDirection': 'wind_gust_direction',
        'maximumWindGustSpeed': 'wind_gust_speed',
        'totalPrecipitationOrTotalWaterEquivalent': 'precipitation',
        'totalSunshine': 'sunshine',
    }

    def parse(self):
        with bz2.open(self.path) as f:
            if not f.read(1):
                return
            f.seek(0)
            message_blocks = json.load(f)['messages']
        for block in message_blocks:
            for message in block[-1]:
                with suppress(SkipRecord):
                    record = self.parse_message(message)
                    self.sanitize_record(record)
                    yield record

    def parse_message(self, message):
        record = {
            'observation_type': 'synop',
        }
        self.parse_tree(record, message)
        is_complete = all(
            record.get(field) is not None for field in self.mandatory_fields)
        if not is_complete:
            self.logger.error("Skipping incomplete record: %s", record)
            raise SkipRecord
        return record

    def parse_tree(self, record, message, base=None):
        data = {} if base is None else base.copy()
        for block in message:
            if isinstance(block, dict):
                key = block['key']
                value = block['value']
                data[key] = value
                if field := self.elements.get(key):
                    record[field] = value
                elif field := self.height_elements.get(key):
                    if data[self.height_field] == self.height:
                        record[field] = value
                elif field := self.time_period_elements.get(key):
                    time_period = data[self.time_period_field]
                    if time_period in self.time_periods:
                        if field == 'sunshine' and value:
                            value *= 60
                        record[field + f'_{-time_period}'] = value
                elif parse_method := getattr(self, f'parse_{key}', None):
                    parse_method(record, data, value)
            else:
                self.parse_tree(record, block, base=data)

    def parse_minute(self, record, data, value):
        parts = ['year', 'month', 'day', 'hour', 'minute']
        if any(data[part] is None for part in parts):
            raise SkipRecord
        record['timestamp'] = datetime.datetime(
            data['year'], data['month'], data['day'], data['hour'],
            data['minute'], tzinfo=tzutc())

    def parse_stationNumber(self, record, data, value):
        if data['stationNumber']:
            wmo_id = f"{data['blockNumber']}{data['stationNumber']:03d}"
        else:
            wmo_id = data['shortStationName']
        record['wmo_station_id'] = wmo_id
        record['dwd_station_id'] = wmo_id_to_dwd(wmo_id)

    def parse_presentWeather(self, record, data, value):
        if record.get('timePeriod'):
            return
        condition = synop_current_weather_code_to_condition(value)
        # Don't overwrite any condition from pastWeather1 with None, but do
        # prioritize presentWeather if it's not None
        if condition:
            record['condition'] = condition

    def parse_pastWeather1(self, record, data, value):
        if value and not record.get('condition'):
            record['condition'] = synop_past_weather_code_to_condition(value)

    def sanitize_record(self, record):
        for field in list(record):
            if field.startswith('precipitation_') and (record[field] or 0) < 0:
                record[field] = None
        if (record.get('cloud_cover') or 0) > 100:
            record['cloud_cover'] = None


class CurrentObservationsParser(Parser):

    PRIORITY = 30

    ELEMENTS = {
        'cloud_cover_total': 'cloud_cover',
        'dew_point_temperature_at_2_meter_above_ground': 'dew_point',
        'dry_bulb_temperature_at_2_meter_above_ground': 'temperature',
        'horizontal_visibility': 'visibility',
        'maximum_wind_speed_last_hour': 'wind_gust_speed',
        'mean_wind_direction_during_last_10 min_at_10_meters_above_ground': (
            'wind_direction'),
        'mean_wind_speed_during last_10_min_at_10_meters_above_ground': (
            'wind_speed'),
        'precipitation_amount_last_hour': 'precipitation',
        'present_weather': 'condition',
        'pressure_reduced_to_mean_sea_level': 'pressure_msl',
        'relative_humidity': 'relative_humidity',
        'total_time_of_sunshine_during_last_hour': 'sunshine',
    }
    DATE_COLUMN = 'surface observations'
    HOUR_COLUMN = 'Parameter description'

    CONVERTERS = {
        'condition': current_observations_weather_code_to_condition,
        'dew_point': celsius_to_kelvin,
        'pressure_msl': hpa_to_pa,
        'sunshine': minutes_to_seconds,
        'temperature': celsius_to_kelvin,
        'visibility': km_to_m,
        'wind_speed': kmh_to_ms,
        'wind_gust_speed': kmh_to_ms,
    }

    def should_skip(self):
        return self.path.endswith(tuple(
            f'{station:_<5}-BEOB.csv'
            for station in settings.IGNORED_CURRENT_OBSERVATIONS_STATIONS))

    def parse(self, lat=None, lon=None, height=None, station_name=None):
        with open(self.path) as f:
            reader = csv.DictReader(f, delimiter=';')
            wmo_station_id = next(reader)[self.DATE_COLUMN].rstrip('_')
            dwd_station_id = wmo_id_to_dwd(wmo_station_id)
            if any(x is None for x in (lat, lon, height, station_name)):
                lat, lon, height, station_name = self.load_location(
                    wmo_station_id)
            # Skip row with German header titles
            next(reader)
            for row in reader:
                yield {
                    'observation_type': 'current',
                    'lat': lat,
                    'lon': lon,
                    'height': height,
                    'dwd_station_id': dwd_station_id,
                    'wmo_station_id': wmo_station_id,
                    'station_name': station_name,
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
        self.sanitize_record(record)
        return record

    def convert_units(self, record):
        for element, converter in self.CONVERTERS.items():
            if record[element] is not None:
                record[element] = converter(record[element])

    def sanitize_record(self, record):
        if record['cloud_cover'] and record['cloud_cover'] > 100:
            self.logger.warning(
                "Ignoring unphysical cloud cover value: %s", record)
            record['cloud_cover'] = None
        if record['relative_humidity'] and record['relative_humidity'] > 100:
            self.logger.warning(
                "Ignoring unphysical relative humidity value: %s", record)
            record['relative_humidity'] = None
        if record['sunshine'] and record['sunshine'] > 3600:
            self.logger.warning(
                "Ignoring unphysical sunshine value: %s", record)
            record['sunshine'] = None

    def load_location(self, wmo_station_id):
        rows = fetch(
            """
            SELECT lat, lon, height, station_name
            FROM sources
            WHERE wmo_station_id = %s
            ORDER BY observation_type DESC, id DESC
            LIMIT 1
            """,
            (wmo_station_id,),
        )
        if not rows:
            raise ValueError(
                f'Unable to find location for WMO station {wmo_station_id}')
        return rows[0]


class ObservationsParser(Parser):

    elements = {}
    converters = {}
    ignored_values = {}

    def should_skip(self):
        if (m := re.search(r'_(\d{8})_(\d{8})_hist\.zip$', str(self.path))):
            end_date = datetime.datetime.strptime(
                m.group(2), '%Y%m%d').replace(tzinfo=tzutc())
            if end_date < settings.MIN_DATE:
                return True
            if settings.MAX_DATE:
                start_date = datetime.datetime.strptime(
                    m.group(1), '%Y%m%d').replace(tzinfo=tzutc())
                if start_date > settings.MAX_DATE:
                    return True
        return False

    def parse(self):
        with zipfile.ZipFile(self.path) as zf:
            dwd_station_id = self.parse_station_id(zf)
            wmo_station_id = dwd_id_to_wmo(dwd_station_id)
            lat_lon_history = self.parse_lat_lon_history(zf, dwd_station_id)
            for record in self.parse_records(zf, lat_lon_history):
                yield {
                    'observation_type': 'historical',
                    'dwd_station_id': dwd_station_id,
                    'wmo_station_id': wmo_station_id,
                    **record
                }

    def parse_station_id(self, zf):
        for filename in zf.namelist():
            if (m := re.match(r'Metadaten_Geographie_(\d+)\.txt', filename)):
                return m.group(1)
        raise ValueError(f"Unable to parse station ID for {self.path}")

    def parse_lat_lon_history(self, zf, dwd_station_id):
        with zf.open(f'Metadaten_Geographie_{dwd_station_id}.txt') as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f, encoding='latin1'),
                delimiter=';')
            history = {}
            for row in reader:
                date_from = datetime.datetime.strptime(
                    row['von_datum'].strip(), '%Y%m%d'
                ).replace(tzinfo=tzutc())
                history[date_from] = (
                    float(row['Geogr.Breite']),
                    float(row['Geogr.Laenge']),
                    float(row['Stationshoehe']),
                    row['Stationsname'])
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
            yield from self.parse_reader(filename, reader, lat_lon_history)

    def parse_reader(self, filename, reader, lat_lon_history):
        for row in reader:
            timestamp = datetime.datetime.strptime(
                row['MESS_DATUM'], '%Y%m%d%H').replace(tzinfo=tzutc())
            if self._skip_timestamp(timestamp):
                continue
            lat, lon, height, station_name = self._station_params(
                timestamp, lat_lon_history)
            yield {
                'source': f'Observations:Recent:{filename}',
                'lat': lat,
                'lon': lon,
                'height': height,
                'station_name': station_name,
                'timestamp': timestamp,
                **self.parse_elements(row, lat, lon, height),
            }

    def _skip_timestamp(self, timestamp):
        return (
            timestamp < settings.MIN_DATE or
            (settings.MAX_DATE and timestamp > settings.MAX_DATE))

    def _station_params(self, timestamp, lat_lon_history):
        info = None
        for date, lat_lon_height_name in lat_lon_history.items():
            if date > timestamp:
                break
            info = lat_lon_height_name
        return info

    def parse_elements(self, row, lat, lon, height):
        elements = {
            element: (
                float(row[element_key])
                if row[element_key].strip() != '-999'
                and row[element_key].strip() not in self.ignored_values.get(
                    element, [])
                else None)
            for element, element_key in self.elements.items()
        }
        for element, converter in self.converters.items():
            if elements[element] is not None:
                elements[element] = converter(elements[element])
        return elements


class CloudCoverObservationsParser(ObservationsParser):

    elements = {
        'cloud_cover': ' V_N',
    }
    ignored_values = {
        'cloud_cover': ['-1', '9'],
    }
    converters = {
        'cloud_cover': eighths_to_percent,
    }


class DewPointObservationsParser(ObservationsParser):

    elements = {
        'dew_point': '  TD',
    }
    converters = {
        'dew_point': celsius_to_kelvin,
    }


class TemperatureObservationsParser(ObservationsParser):

    elements = {
        'relative_humidity': 'RF_TU',
        'temperature': 'TT_TU',
    }
    converters = {
        'temperature': celsius_to_kelvin,
    }


class PrecipitationObservationsParser(ObservationsParser):

    elements = {
        'precipitation': '  R1',
        'condition': 'WRTR',
    }
    converters = {
        'condition': synop_form_of_precipitation_code_to_condition,
    }

    def parse_reader(self, filename, reader, lat_lon_history):
        # XXX: WRTR is missing every third hour, we fill it up from the
        #      previous or next row where sensible
        rows = list(reader)
        for i, row in enumerate(rows):
            if row['WRTR'] != '-999':
                continue
            elif row['RS_IND'].strip() == '0':
                row['WRTR'] = '0'
            elif i > 0 and rows[i-1]['RS_IND'].strip() == '1':
                row['WRTR'] = rows[i-1]['WRTR']
            elif i + 1 < len(rows) and rows[i+1]['RS_IND'].strip() == '1':
                row['WRTR'] = rows[i+1]['WRTR']
            else:
                row['WRTR'] = '9'
        yield from super().parse_reader(filename, rows, lat_lon_history)


class VisibilityObservationsParser(ObservationsParser):

    elements = {
        'visibility': 'V_VV',
    }
    converters = {
        'visibility': int,
    }


class WindObservationsParser(ObservationsParser):

    elements = {
        'wind_speed': '   F',
        'wind_direction': '   D',
    }
    converters = {
        'wind_direction': int,
    }
    ignored_values = {
        'wind_direction': ['990'],
    }


class WindGustsObservationsParser(ObservationsParser):

    META_DATA_URL = (
        'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        'climate/10_minutes/extreme_wind/meta_data/'
        'Meta_Daten_zehn_min_fx_{dwd_station_id}.zip')

    elements = {
        'wind_gust_direction': 'DX_10',
        'wind_gust_speed': 'FX_10',
    }

    def __init__(self, *args, meta_path=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.meta_path = meta_path

    def download(self):
        super().download()
        with zipfile.ZipFile(self.path) as zf:
            dwd_station_id = self.parse_station_id(zf)
        meta_data_url = self.META_DATA_URL.format(
            dwd_station_id=dwd_station_id)
        if not self.meta_path:
            self.meta_path = cache_path(meta_data_url)
        self._download(meta_data_url, self.meta_path)

    def parse_station_id(self, zf):
        for filename in zf.namelist():
            if (m := re.match(r'produkt_.*_(\d+)\.txt', filename)):
                return m.group(1)
        raise ValueError(f"Unable to parse station ID for {self.path}")

    def parse_lat_lon_history(self, zf, dwd_station_id):
        with zipfile.ZipFile(self.meta_path) as meta_zf:
            return super().parse_lat_lon_history(meta_zf, dwd_station_id)

    def parse_reader(self, filename, reader, lat_lon_history):
        hour_values = []
        for row in reader:
            timestamp = datetime.datetime.strptime(
                row['MESS_DATUM'], '%Y%m%d%H%M').replace(tzinfo=tzutc())
            if self._skip_timestamp(timestamp + datetime.timedelta(hours=1)):
                continue
            # Should this be refactored into a base class we will need to
            # properly parse the station parameters and pass them
            values = self.parse_elements(row, None, None, None)
            if values['wind_gust_speed']:
                hour_values.append(values)
            if timestamp.minute == 0:
                yield self._make_record(
                    timestamp, hour_values, filename, lat_lon_history)
                hour_values.clear()

    def _make_record(self, timestamp, hour_values, filename, lat_lon_history):
        lat, lon, height, station_name = self._station_params(
            timestamp, lat_lon_history)
        if hour_values:
            max_value = max(hour_values, key=lambda v: v['wind_gust_speed'])
            direction = max_value['wind_gust_direction']
            speed = max_value['wind_gust_speed']
        else:
            direction = None
            speed = None
        return {
            'source': f'Observations:Recent:{filename}',
            'lat': lat,
            'lon': lon,
            'height': height,
            'station_name': station_name,
            'timestamp': timestamp,
            'wind_gust_direction': direction,
            'wind_gust_speed': speed,
        }


class SunshineObservationsParser(ObservationsParser):

    elements = {
        'sunshine': 'SD_SO',
    }
    converters = {
        'sunshine': minutes_to_seconds,
    }


class PressureObservationsParser(ObservationsParser):

    elements = {
        'pressure_msl': '   P',
        'pressure_station': '  P0',
    }
    converters = {
        'pressure_msl': hpa_to_pa,
        'pressure_station': hpa_to_pa,
    }

    def parse_elements(self, row, lat, lon, height):
        elements = super().parse_elements(row, lat, lon, height)
        if not elements['pressure_msl'] and elements['pressure_station']:
            # Some stations do not record reduced pressure, but do record
            # pressure at station height. We can approximate the pressure at
            # mean sea level through the barometric formula. The error of this
            # approximation could be reduced if we had the current temperature.
            elements['pressure_msl'] = int(round(
                elements['pressure_station']
                * (1 - .0065 * height / 288.15) ** -5.255,
                -1))
        del elements['pressure_station']
        return elements


def get_parser(filename):
    parsers = {
        r'MOSMIX_S_LATEST_240\.kmz$': MOSMIXParser,
        r'Z__C_EDZW_\d+_.*\.json\.bz2$': SYNOPParser,
        r'\w{5}-BEOB\.csv$': CurrentObservationsParser,
        'stundenwerte_FF_': WindObservationsParser,
        'stundenwerte_N_': CloudCoverObservationsParser,
        'stundenwerte_P0_': PressureObservationsParser,
        'stundenwerte_RR_': PrecipitationObservationsParser,
        'stundenwerte_SD_': SunshineObservationsParser,
        'stundenwerte_TD_': DewPointObservationsParser,
        'stundenwerte_TU_': TemperatureObservationsParser,
        'stundenwerte_VV_': VisibilityObservationsParser,
        '10minutenwerte_extrema_wind_': WindGustsObservationsParser,
    }
    for pattern, parser in parsers.items():
        if re.match(pattern, filename):
            return parser
