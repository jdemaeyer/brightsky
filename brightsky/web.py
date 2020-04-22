import falcon
from gunicorn.app.base import BaseApplication

from brightsky import query
from brightsky.utils import parse_date


class WeatherResource:

    def on_get(self, req, resp):
        lat = req.get_param_as_float(
            'lat', required=True, min_value=-90, max_value=90)
        lon = req.get_param_as_float(
            'lon', required=True, min_value=-180, max_value=180)
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
        max_dist = req.get_param_as_int(
            'max_dist', min_value=0, max_value=500000, default=50000)
        rows = query.weather(
            lat, lon, date, last_date=last_date, max_dist=max_dist)
        result = [dict(r) for r in rows]
        for row in result:
            row['timestamp'] = row['timestamp'].isoformat()
        resp.media = {
            'result': result,
        }


app = falcon.API()
app.add_route('/weather', WeatherResource())


class StandaloneApplication(BaseApplication):

    def __init__(self, app, **options):
        self.app = app
        self.options = options
        super().__init__()

    def load_config(self):
        for k, v in self.options.items():
            self.cfg.set(k.lower(), v)

    def load(self):
        return self.app
