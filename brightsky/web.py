import importlib
import sys

import falcon
from dateutil.tz import gettz
from gunicorn.app.base import BaseApplication
from gunicorn.util import import_app

from brightsky import query
from brightsky.units import convert_record, CONVERTERS
from brightsky.utils import parse_date


class BrightskyResource:

    def parse_location(self, req, required=False):
        lat = req.get_param_as_float(
            'lat', required=required, min_value=-90, max_value=90)
        lon = req.get_param_as_float(
            'lon', required=required, min_value=-180, max_value=180)
        return lat, lon

    def parse_max_dist(self, req):
        return req.get_param_as_int(
            'max_dist', min_value=0, max_value=500000, default=50000)

    def parse_date_range(self, req):
        date_str = req.get_param('date', required=True)
        last_date_str = req.get_param('last_date')
        try:
            date = parse_date(date_str)
            if last_date_str:
                last_date = parse_date(last_date_str)
            else:
                last_date = None
        except ValueError:
            raise falcon.HTTPBadRequest(
                description='Please supply dates in ISO 8601 format')
        return date, last_date

    def parse_timezone(self, req):
        tz_str = req.get_param('tz')
        if not tz_str:
            return
        tz = gettz(tz_str)
        if not tz:
            raise falcon.HTTPBadRequest(
                description='Unknown timezone: %s' % tz_str)
        return tz


class WeatherResource(BrightskyResource):

    ALLOWED_UNITS = ['si'] + list(CONVERTERS)

    def on_get(self, req, resp):
        date, last_date = self.parse_date_range(req)
        lat, lon = self.parse_location(req)
        station_id = req.get_param('station_id')
        source_id = req.get_param_as_int('source_id')
        max_dist = self.parse_max_dist(req)
        units = req.get_param('units', default='dwd').lower()
        if units not in self.ALLOWED_UNITS:
            raise falcon.HTTPBadRequest(
                description="'units' must be in %s" % (self.ALLOWED_UNITS,))
        timezone = self.parse_timezone(req)
        if timezone:
            if not date.tzinfo:
                date = date.replace(tzinfo=timezone)
            if last_date and not last_date.tzinfo:
                last_date = last_date.replace(tzinfo=timezone)
        elif date.tzinfo:
            timezone = date.tzinfo
        try:
            result = query.weather(
                date, last_date=last_date, lat=lat, lon=lon,
                station_id=station_id, source_id=source_id, max_dist=max_dist)
        except ValueError as e:
            raise falcon.HTTPBadRequest(description=str(e))
        for row in result['weather']:
            self.process_row(row, units, timezone)
        resp.media = result

    def process_row(self, row, units, timezone):
        if timezone:
            row['timestamp'] = row['timestamp'].astimezone(timezone)
        row['timestamp'] = row['timestamp'].isoformat()
        if units != 'si':
            convert_record(row, units)


class SourcesResource(BrightskyResource):

    def on_get(self, req, resp):
        lat, lon = self.parse_location(req)
        max_dist = self.parse_max_dist(req)
        station_id = req.get_param('station_id')
        source_id = req.get_param_as_int('source_id')
        try:
            result = query.sources(
                lat=lat, lon=lon, station_id=station_id, source_id=source_id,
                max_dist=max_dist)
        except ValueError as e:
            raise falcon.HTTPBadRequest(description=str(e))
        resp.media = result


app = falcon.API()
app.add_route('/weather', WeatherResource())
app.add_route('/sources', SourcesResource())


class StandaloneApplication(BaseApplication):

    def __init__(self, app_uri, **options):
        self.app_uri = app_uri
        self.options = options
        super().__init__()

    def load_config(self):
        for k, v in self.options.items():
            self.cfg.set(k.lower(), v)

    def load(self):
        brightsky_mods = [
            mod for name, mod in sys.modules.items()
            if name.startswith('brightsky.')]
        for mod in brightsky_mods:
            importlib.reload(mod)
        return import_app(self.app_uri)
