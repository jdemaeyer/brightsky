import csv
import datetime
import re

import dwdparse.parsers
import numpy as np
from dateutil.tz import tzutc
from isal import isal_zlib as zlib

from brightsky.db import fetch
from brightsky.export import (
    AlertExporter,
    DBExporter,
    RADOLANExporter,
    SYNOPExporter,
)
from brightsky.settings import settings


class BrightSkyMixin:

    PRIORITY = 10
    exporter = DBExporter

    def skip_path(self, path):
        return False


class ObservationsBrightSkyMixin(BrightSkyMixin):

    def skip_path(self, path):
        if (m := re.search(r'_(\d{8})_(\d{8})_hist\.zip$', str(path))):
            end_date = datetime.datetime.strptime(
                m.group(2),
                '%Y%m%d',
            ).replace(tzinfo=tzutc())
            if end_date < settings.MIN_DATE:
                return True
            if settings.MAX_DATE:
                start_date = datetime.datetime.strptime(
                    m.group(1), '%Y%m%d').replace(tzinfo=tzutc())
                if start_date > settings.MAX_DATE:
                    return True
        return False

    def skip_timestamp(self, timestamp):
        if timestamp < settings.MIN_DATE:
            return True
        elif settings.MAX_DATE and timestamp > settings.MAX_DATE:
            return True
        return False


class MOSMIXParser(BrightSkyMixin, dwdparse.parsers.MOSMIXParser):

    PRIORITY = 20


class SYNOPParser(BrightSkyMixin, dwdparse.parsers.SYNOPParser):

    PRIORITY = 30
    exporter = SYNOPExporter


class CurrentObservationsParser(
    BrightSkyMixin,
    dwdparse.parsers.CurrentObservationsParser,
):

    PRIORITY = 30

    def skip_path(self, path):
        return path.endswith(tuple(
            f'{station:_<5}-BEOB.csv'
            for station in settings.IGNORED_CURRENT_OBSERVATIONS_STATIONS
        ))

    def parse(self, path, lat=None, lon=None, height=None, station_name=None):
        if any(x is None for x in (lat, lon, height, station_name)):
            with open(path) as f:
                reader = csv.DictReader(f, delimiter=';')
                wmo_station_id = next(reader)[self.DATE_COLUMN].rstrip('_')
            lat, lon, height, station_name = self._load_location(
                wmo_station_id,
            )
        return super().parse(
            path,
            lat=lat,
            lon=lon,
            height=height,
            station_name=station_name,
        )

    def _load_location(self, wmo_station_id):
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
            raise ValueError(f'Cannot find location for WMO {wmo_station_id}')
        return rows[0]


class CloudCoverObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.CloudCoverObservationsParser,
):
    pass


class DewPointObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.DewPointObservationsParser,
):
    pass


class TemperatureObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.TemperatureObservationsParser,
):
    pass


class PrecipitationObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.PrecipitationObservationsParser,
):
    pass


class SolarRadiationObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.SolarRadiationObservationsParser,
):

    def skip_timestamp(self, timestamp):
        # We aggregate solar radiation from ten-minute data, where the values
        # correspond to radiation for the NEXT ten minutes, i.e. the value
        # tagged 14:30 contains the solar radiation between 14:30 - 14:40 (I
        # have not found a place where this is officially documented, but this
        # interpretation makes the values align with the hourly data from the
        # 'current' sources).
        # This makes solar radiation the only parameter where the 'recent'
        # sources produce a data point for today, which is otherwise only
        # served by the 'current' sources. To avoid excessive fill-up when
        # querying today's weather, we ignore this last data point (but will
        # pick it up on the next day).
        if timestamp.date() == datetime.date.today():
            return True
        return super().skip_timestamp(timestamp)


class VisibilityObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.VisibilityObservationsParser,
):
    pass


class WindObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.WindObservationsParser,
):
    pass


class WindGustsObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.WindGustsObservationsParser,
):
    pass


class SunshineObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.SunshineObservationsParser,
):
    pass


class PressureObservationsParser(
    ObservationsBrightSkyMixin,
    dwdparse.parsers.PressureObservationsParser,
):
    pass


class RADOLANParser(BrightSkyMixin, dwdparse.parsers.RADOLANParser):

    PRIORITY = 30
    exporter = RADOLANExporter

    def process_raw_data(self, raw):
        # XXX: Unlike with the other weather parameters, because of it's large
        #      size, we're storing the radar data in a half-raw state and
        #      performing some final processing during runtime. This brings
        #      down the response time for retrieving one full radar scan
        #      (single timestamp, 1200x1100 pixels) from 1.5 seconds to about
        #      1 ms, mainly because of the reduced data transfer when fetching
        #      the scan from the database. An important caveat of this is that
        #      we are replacing `None` with `0`!
        data = np.array(raw, dtype='i2')
        data[data > 4095] = 0
        data = np.flipud(data.reshape((1200, 1100)))
        return zlib.compress(np.ascontiguousarray(data))


class CAPParser(BrightSkyMixin, dwdparse.parsers.CAPParser):

    PRIORITY = 40
    exporter = AlertExporter


def get_parser(filename):
    parsers = {
        r'DE1200_RV': RADOLANParser,
        r'MOSMIX_(S|L)_LATEST(_240)?\.kmz$': MOSMIXParser,
        r'Z_CAP_C_EDZW_LATEST_.*_COMMUNEUNION_MUL\.zip': CAPParser,
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
        '10minutenwerte_SOLAR_': SolarRadiationObservationsParser,
    }
    for pattern, parser in parsers.items():
        if re.match(pattern, filename):
            return parser
