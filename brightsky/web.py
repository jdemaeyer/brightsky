import importlib
import sys
from contextlib import contextmanager

import falcon
import falcon_cors
from dateutil.tz import gettz
from falcon.errors import HTTPInvalidParam
from gunicorn.app.base import BaseApplication
from gunicorn.util import import_app

import brightsky
from brightsky import query
from brightsky.settings import settings
from brightsky.units import convert_record, CONVERTERS
from brightsky.utils import parse_date, sunrise_sunset


@contextmanager
def convert_exceptions():
    try:
        yield
    except ValueError as e:
        raise falcon.HTTPBadRequest(description=str(e))
    except LookupError as e:
        raise falcon.HTTPNotFound(description=str(e))


class BrightskyResource:

    ALLOWED_UNITS = ['si'] + list(CONVERTERS)

    def parse_location(self, req, required=False):
        lat = req.get_param_as_float(
            'lat', required=required, min_value=-90, max_value=90)
        lon = req.get_param_as_float(
            'lon', required=required, min_value=-180, max_value=180)
        if lat != lat:
            raise HTTPInvalidParam('The value cannot be NaN', 'lat')
        elif lon != lon:
            raise HTTPInvalidParam('The value cannot be NaN', 'lon')
        return lat, lon

    def parse_source_ids(self, req):
        return (
            self._parse_list_or_single(req, 'source_id', transform=int),
            self._parse_list_or_single(req, 'dwd_station_id'),
            self._parse_list_or_single(req, 'wmo_station_id'))

    def _parse_list_or_single(self, req, name, transform=None):
        value = req.get_param_as_list(name, transform=transform)
        if value and len(value) == 1:
            return value[0]
        return value

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
        except (ValueError, OverflowError):
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

    def parse_units(self, req):
        units = req.get_param('units', default='dwd').lower()
        if units not in self.ALLOWED_UNITS:
            raise falcon.HTTPBadRequest(
                description="'units' must be in %s" % (self.ALLOWED_UNITS,))
        return units

    def process_timestamp(self, row, key, timezone):
        if not row[key]:
            return
        if timezone:
            row[key] = row[key].astimezone(timezone)
        row[key] = row[key].isoformat()

    def process_sources(self, sources, timezone=None):
        for source in sources:
            self.process_timestamp(source, 'first_record', timezone)
            self.process_timestamp(source, 'last_record', timezone)


class WeatherResource(BrightskyResource):

    PRECIPITATION_FIELD = 'precipitation'
    WIND_SPEED_FIELD = 'wind_speed'

    def on_get(self, req, resp):
        date, last_date = self.parse_date_range(req)
        lat, lon = self.parse_location(req)
        source_id, dwd_station_id, wmo_station_id = self.parse_source_ids(req)
        # TODO: Remove this fallback on 2020-06-13
        if not wmo_station_id:
            wmo_station_id = req.get_param('station_id')
        max_dist = self.parse_max_dist(req)
        timezone = self.parse_timezone(req)
        units = self.parse_units(req)
        if timezone:
            if not date.tzinfo:
                date = date.replace(tzinfo=timezone)
            if last_date and not last_date.tzinfo:
                last_date = last_date.replace(tzinfo=timezone)
        elif date.tzinfo:
            timezone = date.tzinfo
        with convert_exceptions():
            result = self.query(
                date, last_date=last_date, lat=lat, lon=lon,
                dwd_station_id=dwd_station_id, wmo_station_id=wmo_station_id,
                source_id=source_id, max_dist=max_dist)
        self.process_sources(result['sources'])
        source_map = {s['id']: s for s in result['sources']}
        for row in result['weather']:
            self.process_row(row, units, timezone, source_map)
        resp.media = result

    def query(self, *args, **kwargs):
        return query.weather(*args, **kwargs)

    def process_row(self, row, units, timezone, source_map):
        row['icon'] = self.get_icon(row, source_map)
        if units != 'si':
            convert_record(row, units)
        self.process_timestamp(row, 'timestamp', timezone)

    def get_icon(self, row, source_map):
        if row['condition'] in (
                'fog', 'sleet', 'snow', 'hail', 'thunderstorm'):
            return row['condition']
        precipitation = row[self.PRECIPITATION_FIELD]
        wind_speed = row[self.WIND_SPEED_FIELD]
        # Don't show 'rain' icon for little precipitation, and do show 'rain'
        # icon when condition is None but there is significant precipitation
        is_rainy = (
            row['condition'] == 'rain' and precipitation is None) or (
            (precipitation or 0) > settings.ICON_RAIN_THRESHOLD)
        if is_rainy:
            return 'rain'
        elif (wind_speed or 0) > settings.ICON_WIND_THRESHOLD:
            return 'wind'
        elif (row['cloud_cover'] or 0) >= settings.ICON_CLOUDY_THRESHOLD:
            return 'cloudy'
        source = source_map[row['source_id']]
        try:
            sunrise, sunset = sunrise_sunset(
                source['lat'], source['lon'], row['timestamp'].date())
        except ValueError as e:
            daytime = 'day' if 'above' in e.args[0] else 'night'
        else:
            daytime = (
                'day' if sunrise <= row['timestamp'] <= sunset else 'night')
        if (row['cloud_cover'] or 0) >= settings.ICON_PARTLY_CLOUDY_THRESHOLD:
            return f'partly-cloudy-{daytime}'
        return f'clear-{daytime}'


class CurrentWeatherResource(WeatherResource):

    PRECIPITATION_FIELD = 'precipitation_10'
    WIND_SPEED_FIELD = 'wind_speed_10'

    def on_get(self, req, resp):
        lat, lon = self.parse_location(req)
        source_id, dwd_station_id, wmo_station_id = self.parse_source_ids(req)
        max_dist = self.parse_max_dist(req)
        timezone = self.parse_timezone(req)
        units = self.parse_units(req)
        with convert_exceptions():
            result = query.current_weather(
                lat=lat, lon=lon, dwd_station_id=dwd_station_id,
                wmo_station_id=wmo_station_id, source_id=source_id,
                max_dist=max_dist)
        self.process_sources(result['sources'])
        source_map = {s['id']: s for s in result['sources']}
        self.process_row(result['weather'], units, timezone, source_map)
        resp.media = result


class SynopResource(WeatherResource):

    PRECIPITATION_FIELD = 'precipitation_10'
    WIND_SPEED_FIELD = 'wind_speed_10'

    def query(self, *args, **kwargs):
        kwargs.pop('max_dist')
        if any(kwargs.pop(param) for param in ['lat', 'lon']):
            raise falcon.HTTPBadRequest(
                "Querying by lat/lon is not supported for the synop endpoint")
        return query.synop(*args, **kwargs)


class SourcesResource(BrightskyResource):

    def on_get(self, req, resp):
        lat, lon = self.parse_location(req)
        max_dist = self.parse_max_dist(req)
        source_id, dwd_station_id, wmo_station_id = self.parse_source_ids(req)
        with convert_exceptions():
            result = query.sources(
                lat=lat, lon=lon, dwd_station_id=dwd_station_id,
                wmo_station_id=wmo_station_id, source_id=source_id,
                max_dist=max_dist, ignore_type=True)
        self.process_sources(result.get('sources', []))
        resp.media = result


class StatusResource:

    def on_get(self, req, resp):
        resp.media = {
            'name': 'brightsky',
            'version': brightsky.__version__,
            'status': 'ok',
        }

    def on_head(self, req, resp):
        pass


cors = falcon_cors.CORS(
    allow_origins_list=settings.CORS_ALLOWED_ORIGINS,
    allow_all_origins=settings.CORS_ALLOW_ALL_ORIGINS,
    allow_headers_list=settings.CORS_ALLOWED_HEADERS,
    allow_all_headers=settings.CORS_ALLOW_ALL_HEADERS,
    allow_all_methods=True)

app = falcon.API(middleware=[cors.middleware])
app.req_options.auto_parse_qs_csv = True

app.add_route('/', StatusResource())
app.add_route('/weather', WeatherResource())
app.add_route('/current_weather', CurrentWeatherResource())
app.add_route('/synop', SynopResource())
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
