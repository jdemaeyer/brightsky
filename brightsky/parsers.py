import csv
import datetime
import re

import dwdparse.parsers
from dateutil.tz import tzutc

from brightsky.db import fetch
from brightsky.export import DBExporter, SYNOPExporter
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


def get_parser(filename):
    parsers = {
        r'MOSMIX_(S|L)_LATEST(_240)?\.kmz$': MOSMIXParser,
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
