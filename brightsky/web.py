import falcon

from brightsky import query
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


class WeatherResource(BrightskyResource):

    def on_get(self, req, resp):
        lat, lon = self.parse_location(req, required=True)
        date, last_date = self.parse_date_range(req)
        max_dist = self.parse_max_dist(req)
        rows = query.weather(
            lat, lon, date, last_date=last_date, max_dist=max_dist)
        result = [dict(r) for r in rows]
        for row in result:
            row['timestamp'] = row['timestamp'].isoformat()
        resp.media = {
            'result': result,
        }


class SourcesResource(BrightskyResource):

    def on_get(self, req, resp):
        lat, lon = self.parse_location(req)
        max_dist = self.parse_max_dist(req)
        station_id = req.get_param('station_id')
        source_id = req.get_param_as_int('source_id')
        try:
            rows = query.sources(
                lat=lat, lon=lon, station_id=station_id, source_id=source_id,
                max_dist=max_dist)
        except ValueError as e:
            raise falcon.HTTPBadRequest(description=str(e))
        result = [dict(r) for r in rows]
        resp.media = {
            'result': result,
        }


app = falcon.API()
app.add_route('/weather', WeatherResource())
app.add_route('/sources', SourcesResource())
