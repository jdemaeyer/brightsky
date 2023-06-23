import base64
import datetime
import zlib

import falcon
import numpy as np
import pytest
from dateutil.tz import tzutc

import brightsky
from brightsky.export import DBExporter, SYNOPExporter
from brightsky.parsers import CAPParser, RADOLANParser
from brightsky.query import _warn_cells
from brightsky.web import make_app

from .utils import settings


SOURCES = [
    # Ordered by their distance to (52, 7.6)
    {
        'observation_type': 'forecast',
        'lat': 51.97,
        'lon': 7.63,
        'height': 60,
        'station_name': 'MUENSTER ZENTRUM',
        'dwd_station_id': None,
        'wmo_station_id': 'P0036',
    },
    {
        'observation_type': 'current',
        'lat': 52.13,
        'lon': 7.7,
        'height': 48,
        'station_name': 'MUENSTER/OSNABR.',
        'dwd_station_id': '01766',
        'wmo_station_id': '10315',
    },
    {
        'observation_type': 'historical',
        'lat': 52.1344,
        'lon': 7.6969,
        'height': 47.8,
        'station_name': 'Münster/Osnabrück',
        'dwd_station_id': '01766',
        'wmo_station_id': '10315',
    },
]
RECENT_RECORDS = [
    {
        'timestamp': (
            datetime.datetime(2020, 8, 19, 12, tzinfo=tzutc())
            + datetime.timedelta(hours=i)),
        **SOURCES[2],
        'temperature': 100 + i,
    }
    for i in range(40)
]
CURRENT_RECORDS = [
    {
        'timestamp': (
            datetime.datetime(2020, 8, 20, 12, tzinfo=tzutc())
            + datetime.timedelta(hours=i)),
        **SOURCES[1],
        'temperature': 200 + i,
    }
    for i in range(24)
]
FORECAST_RECORDS = [
    {
        'timestamp': (
            datetime.datetime(2020, 8, 21, 8, tzinfo=tzutc())
            + datetime.timedelta(hours=i)),
        **SOURCES[0],
        'temperature': 300 + i,
    }
    for i in range(40)
]

ALL_FIELDS_RECORD = {
    'temperature': 294.35,
    'precipitation': 1.4,
    'pressure_msl': 100620,
    'sunshine': 1740,
    'wind_direction': 130,
    'wind_speed': 2.2,
    'cloud_cover': 87,
    'dew_point': 287.65,
    'visibility': 40000,
    'wind_gust_direction': 130,
    'wind_gust_speed': 3.9,
    'condition': 'fog',
}
RECENT_RECORDS[12].update(ALL_FIELDS_RECORD)

CONDITION_FIELDS = [
    {'condition': 'fog', '_expected_icon': 'fog'},
    {'condition': 'dry', '_expected_icon': 'clear-night'},
    {'condition': 'dry', 'cloud_cover': 50.,
     '_expected_icon': 'partly-cloudy-night'},
    {'condition': 'sleet', '_expected_icon': 'sleet'},
    {'condition': 'snow', '_expected_icon': 'snow'},
    {'condition': 'hail', '_expected_icon': 'hail'},
    {'condition': 'thunderstorm', '_expected_icon': 'thunderstorm'},
    {'condition': 'dry', '_expected_icon': 'clear-day'},
    {'condition': 'rain', '_expected_icon': 'rain'},
    {'condition': 'dry', 'cloud_cover': 50.,
     '_expected_icon': 'partly-cloudy-day'},
    {'condition': 'dry', 'cloud_cover': 100., '_expected_icon': 'cloudy'},
    {'precipitation': 0.3, '_expected_icon': 'clear-day'},
    {'precipitation': 10., '_expected_icon': 'rain'},
]
for i, fields in enumerate(CONDITION_FIELDS):
    RECENT_RECORDS[12+i].update(fields)


SYNOP_SOURCE = {
    'observation_type': 'synop',
    'lat': 52.1344,
    'lon': 7.69685,
    'height': 47.8,
    'station_name': 'Muenster/Osnabrueck',
    'dwd_station_id': '01766',
    'wmo_station_id': '10315',
}
SYNOP_NOW = datetime.datetime.utcnow().replace(
    minute=0, second=0, microsecond=0, tzinfo=tzutc()
) + datetime.timedelta(hours=1)
SYNOP_RECORDS = [
    {
        'timestamp': SYNOP_NOW - datetime.timedelta(minutes=90),
        **SYNOP_SOURCE,
        'temperature': 300,
    },
    {
        'timestamp': SYNOP_NOW - datetime.timedelta(minutes=50),
        **SYNOP_SOURCE,
        'precipitation_10': 0.2,
        'wind_speed_10': 2.0,
        'wind_direction_10': 10,
        'wind_gust_speed_10': 2.3,
        'wind_gust_direction_10': 130,
    },
    {
        'timestamp': SYNOP_NOW - datetime.timedelta(minutes=40),
        **SYNOP_SOURCE,
        'precipitation_10': 0.3,
        'wind_speed_10': 1.8,
        'wind_direction_10': 350,
        'wind_gust_speed_10': 5.7,
        'wind_gust_direction_10': 60,
    },
    {
        'timestamp': SYNOP_NOW - datetime.timedelta(minutes=30),
        **SYNOP_SOURCE,
        'cloud_cover': None,
        'dew_point': 292.17,
        'precipitation_10': 0.,
        'pressure_msl': 100880,
        'relative_humidity': 80,
        'sunshine_30': 1440,
        'temperature': 296.15,
        'visibility': 35000,
        'wind_direction_10': 340,
        'wind_speed_10': 1.2,
        'wind_gust_direction_10': 20,
        'wind_gust_speed_10': 2.1,
    },
    {
        'timestamp': SYNOP_NOW - datetime.timedelta(minutes=20),
        **SYNOP_SOURCE,
        'precipitation_10': 0.,
        'wind_speed_10': 0.8,
        'wind_direction_10': 30,
        'wind_gust_speed_10': 3.3,
        'wind_gust_direction_10': 30,
    },
    {
        'timestamp': SYNOP_NOW - datetime.timedelta(minutes=10),
        **SYNOP_SOURCE,
        'precipitation_10': 0.1,
        'wind_speed_10': 2.2,
        'wind_direction_10': 20,
        'wind_gust_speed_10': 5.1,
        'wind_gust_direction_10': 40,
    },
    {
        'timestamp': SYNOP_NOW,
        **SYNOP_SOURCE,
        'cloud_cover': 88,
        'dew_point': 292.47,
        'precipitation_10': 0,
        'pressure_msl': 100920,
        'relative_humidity': 80,
        'sunshine_60': 3000,
        'temperature': 296.05,
        'wind_direction_10': 350,
        'wind_speed_10': 2.1,
        'wind_gust_direction_10': 30,
        'wind_gust_speed_10': 3.1,
        'condition': 'rain',
    },
]


@pytest.fixture
def data(db):
    records = RECENT_RECORDS + CURRENT_RECORDS + FORECAST_RECORDS
    DBExporter().export(records)


@pytest.fixture
def synop_data(db):
    SYNOPExporter().export(SYNOP_RECORDS)


@pytest.fixture
def radar_data(db, data_dir):
    p = RADOLANParser()
    p.exporter().export(p.parse(data_dir / 'DE1200_RV2305081330.tar.bz2'))


@pytest.fixture
def alerts_data(db, data_dir):
    p = CAPParser()
    fn = 'Z_CAP_C_EDZW_LATEST_PVW_STATUS_PREMIUMDWD_COMMUNEUNION_MUL.zip'
    p.exporter().export(p.parse(data_dir / fn))
    _warn_cells.CELLS_CACHE_PATH = data_dir / 'alert_cells.json'


def test_sources_required_parameters(data, api):
    assert api.simulate_get('/sources').status_code == 400
    assert api.simulate_get('/sources?lat=52').status_code == 400
    assert api.simulate_get('/sources?lon=7.6').status_code == 400
    assert api.simulate_get('/sources?lat=52&lon=7.6').status_code == 200
    assert api.simulate_get('/sources?wmo_station_id=10315').status_code == 200
    assert api.simulate_get('/sources?dwd_station_id=01766').status_code == 200


def test_sources_response(data, api):
    resp = api.simulate_get('/sources?lat=52&lon=7.6')
    assert resp.status_code == 200
    resp_sources = resp.json['sources']
    assert len(resp_sources) == 3
    for resp_source, source in zip(resp_sources, SOURCES):
        for k, v in source.items():
            assert resp_source[k] == v
    assert [s['distance'] for s in resp.json['sources']] == [
        3922, 16008, 16365]
    assert resp_sources[0]['first_record'] == (
        FORECAST_RECORDS[0]['timestamp'].isoformat())
    assert resp_sources[0]['last_record'] == (
        FORECAST_RECORDS[-1]['timestamp'].isoformat())


def test_sources_max_dist(data, api):
    resp = api.simulate_get('/sources?lat=52&lon=7.6')
    assert len(resp.json['sources']) == 3
    resp = api.simulate_get('/sources?lat=52&lon=7.6&max_dist=5000')
    assert len(resp.json['sources']) == 1


def test_sources_by_station_id(data, api):
    paths = [
        '/sources?wmo_station_id=10315',
        '/sources?dwd_station_id=01766',
    ]
    for path in paths:
        resp = api.simulate_get(path)
        assert resp.status_code == 200
        assert len(resp.json['sources']) == 2


def test_source_by_source_id(db, data, api):
    source_id = db.fetch(
        "SELECT id FROM sources WHERE observation_type = 'forecast'")[0]['id']
    resp = api.simulate_get(f'/sources?source_id={source_id}')
    assert len(resp.json['sources']) == 1
    assert resp.json['sources'][0]['observation_type'] == 'forecast'


def test_no_sources_available(data, api):
    assert api.simulate_get('/sources?lat=0&lon=0').status_code == 404
    assert api.simulate_get('/sources?wmo_station_id=12345').status_code == 404
    assert api.simulate_get('/sources?dwd_station_id=12345').status_code == 404


def test_weather_required_parameters(data, api):
    assert api.simulate_get('/weather').status_code == 400
    assert api.simulate_get('/weather?lat=52&lon=7.6').status_code == 400
    assert api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20').status_code == 200
    assert api.simulate_get(
        '/weather?lat=52&lon=7.6&last_date=2020-08-20').status_code == 400


def test_weather_response(data, api):
    resp = api.simulate_get('/weather?lat=52&lon=7.6&date=2020-08-20')
    assert len(resp.json['weather']) == 25
    assert len(resp.json['sources']) == 1
    assert all(
        w['source_id'] == resp.json['sources'][0]['id']
        for w in resp.json['weather'])
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T00:00:00+00:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-21T00:00:00+00:00'
    for w, record in zip(resp.json['weather'], RECENT_RECORDS[12:]):
        assert w['temperature'] == round(record['temperature'] - 273.15, 2)


def test_weather_date_range(data, api):
    resp = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20&last_date=2020-08-22')
    assert len(resp.json['weather']) == 49
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T00:00:00+00:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-22T00:00:00+00:00'
    resp = api.simulate_get('/weather?lat=52&lon=7.6&date=2020-08-20T12:00')
    assert len(resp.json['weather']) == 25
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T12:00:00+00:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-21T12:00:00+00:00'


def test_weather_source_selection(data, api):
    resp = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20&last_date=2020-08-22')
    assert len(resp.json['sources']) == 3
    observation_types = {
        s['id']: s['observation_type'] for s in resp.json['sources']}
    for w in resp.json['weather'][:28]:
        assert observation_types[w['source_id']] == 'historical'
    for w in resp.json['weather'][28:36]:
        assert observation_types[w['source_id']] == 'current'
    for w in resp.json['weather'][36:]:
        assert observation_types[w['source_id']] == 'forecast'


def test_weather_units(data, api):
    resp_dwd = api.simulate_get('/weather?lat=52&lon=7.6&date=2020-08-20')
    resp_si = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20&units=si')
    expected_conversions = {
        'temperature': 21.2,
        'pressure_msl': 1006.2,
        'sunshine': 29,
        'wind_speed': 7.9,
        'dew_point': 14.5,
        'wind_gust_speed': 14.,
    }
    for k, v in ALL_FIELDS_RECORD.items():
        assert resp_dwd.json['weather'][0][k] == expected_conversions.get(k, v)
        assert resp_si.json['weather'][0][k] == v


def test_weather_timezone(data, api):
    resp = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20&tz=Europe/Berlin')
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T00:00:00+02:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-21T00:00:00+02:00'
    # date offset should be used if tz not supplied
    resp = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20T00:00:00%2b02:00')
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T00:00:00+02:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-21T00:00:00+02:00'
    # but tz should always be preferred
    resp = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20T00:00:00-04:00'
        '&tz=Europe/Berlin')
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T06:00:00+02:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-21T06:00:00+02:00'
    # last_date's offset should be used for range selection but not as response
    # timezone
    resp = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20'
        '&last_date=2020-08-20T12:00-04:00')
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T00:00:00+00:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-20T16:00:00+00:00'
    # date's offset should NOT be used as default last_date offset
    resp = api.simulate_get(
        '/weather?lat=52&lon=7.6&date=2020-08-20T00:00:00%2b02:00'
        '&last_date=2020-08-22')
    assert resp.json['weather'][0]['timestamp'] == '2020-08-20T00:00:00+02:00'
    assert resp.json['weather'][-1]['timestamp'] == '2020-08-22T02:00:00+02:00'


def test_weather_icon(data, api):
    resp = api.simulate_get('/weather?lat=52&lon=7.6&date=2020-08-20')
    for condition, record in zip(CONDITION_FIELDS, resp.json['weather']):
        assert record['icon'] == condition['_expected_icon']


def test_synop_disallows_lat_lon(data, api):
    resp = api.simulate_get(
        '/synop',
        params={
            'lat': 52,
            'lon': 7.6,
            'date': SYNOP_RECORDS[0]['timestamp'].isoformat(),
        })
    assert resp.status_code == 400


def test_synop_response(synop_data, api):
    resp = api.simulate_get(
        '/synop',
        params={
            'wmo_station_id': SYNOP_SOURCE['wmo_station_id'],
            'date': SYNOP_RECORDS[0]['timestamp'].isoformat(),
        })
    assert resp.status_code == 200
    assert len(resp.json['sources']) == 1
    assert len(resp.json['weather']) == len(SYNOP_RECORDS)
    for w, record in zip(resp.json['weather'], SYNOP_RECORDS):
        assert w['timestamp'] == record['timestamp'].isoformat()


def test_current_weather_response(synop_data, api, db):
    # XXX: This test may be flaky as the concurrent refresh may not have
    #      finished yet. Can we somehow wait until the lock is released?
    resp = api.simulate_get('/current_weather?lat=52&lon=7.6')
    assert resp.status_code == 200
    assert len(resp.json['sources']) == 1
    expected_weather = {
        # From latest record
        'timestamp': SYNOP_NOW.isoformat(),
        'cloud_cover': 88,
        'condition': 'rain',
        'dew_point': 19.32,
        'precipitation_10': 0.,
        'pressure_msl': 1009.2,
        'relative_humidity': 80,
        'temperature': 22.9,
        'wind_direction_10': 350,
        'wind_speed_10': 7.6,
        'wind_gust_direction_10': 30,
        'wind_gust_speed_10': 11.2,
        # Summed from latest three/six records
        'precipitation_30': 0.1,
        'precipitation_60': 0.6,
        # Averaged from latest three/six records
        'wind_speed_30': 6.1,
        'wind_speed_60': 6.1,
        # Averaged (for circular quantities) from latest three/six records
        'wind_direction_30': 13,
        'wind_direction_60': 3,
        # Filled up from previous record
        'visibility': 35000,
        # Maximum from previous three/six records
        'wind_gust_direction_30': 40,
        'wind_gust_direction_60': 60,
        'wind_gust_speed_30': 18.4,
        'wind_gust_speed_60': 20.5,
        # Calculated from latest available _30 and _60 values
        'sunshine_30': 26,
        'sunshine_60': 50,
    }
    for k, v in expected_weather.items():
        assert resp.json['weather'][k] == v, k


def test_radar_response(radar_data, api):
    resp = api.simulate_get('/radar?date=2023-05-08T11:30')
    assert resp.status_code == 200
    assert len(resp.json['radar']) == 1
    assert api.simulate_get('/radar?lat=52').status_code == 400
    assert api.simulate_get('/radar?lon=7.6').status_code == 400
    assert api.simulate_get('/radar?lat=52&lon=7.6').status_code == 200


def _get_radar_data(api, fmt, bbox=False):
    url = f'/radar?date=2023-05-08T13:30&format={fmt}'
    if bbox:
        url += '&bbox=1117,334,1121,338'
    resp = api.simulate_get(url, headers={'Accept-Encoding': 'gzip'})
    assert resp.status_code == 200
    return resp.json['radar'][0]['precipitation_5']


def _check_radar_data(data):
    if len(data) == 5:
        clip = data
    else:
        assert sum(sum(row) for row in data) == 564030
        clip = [
            row[334:339]
            for row in data[1117:1122]
        ]
    assert clip == [
        [3, 5, 2, 1, 3],
        [2, 3, 3, 0, 0],
        [3, 4, 1, 0, 3],
        [0, 8, 0, 0, 0],
        [0, 0, 0, 0, 0],
    ]


def test_radar_response_compressed(radar_data, api):
    raw = _get_radar_data(api, 'compressed')
    data = np.frombuffer(
        zlib.decompress(base64.b64decode(raw)),
        dtype='i2',
    ).reshape(
        (1200, 1100),
    ).tolist()
    _check_radar_data(data)
    raw = _get_radar_data(api, 'compressed', bbox=True)
    clip = np.frombuffer(
        zlib.decompress(base64.b64decode(raw)),
        dtype='i2',
    ).reshape(
        (5, 5),
    ).tolist()
    _check_radar_data(clip)


def test_radar_response_bytes(radar_data, api):
    raw = _get_radar_data(api, 'bytes')
    data = np.frombuffer(
        base64.b64decode(raw),
        dtype='i2',
    ).reshape(
        (1200, 1100),
    ).tolist()
    _check_radar_data(data)
    raw = _get_radar_data(api, 'bytes', bbox=True)
    clip = np.frombuffer(
        base64.b64decode(raw),
        dtype='i2',
    ).reshape(
        (5, 5),
    ).tolist()
    _check_radar_data(clip)


def test_radar_response_plain(radar_data, api):
    data = _get_radar_data(api, 'plain')
    _check_radar_data(data)
    clip = _get_radar_data(api, 'plain', bbox=True)
    _check_radar_data(clip)


def test_radar_response_geometry(radar_data, api):
    resp = api.simulate_get('/radar?date=2023-05-08T11:30')
    assert resp.json['geometry']['type'] == 'Polygon'
    assert 'latlon_position' not in resp.json
    expected_coords = [
        (1.4633, 55.86209),
        (3.56699, 45.69643),
        (16.58087, 45.68461),
        (18.73162, 55.84544),
    ]
    for p, exp_p in zip(resp.json['geometry']['coordinates'], expected_coords):
        assert p[0] == pytest.approx(exp_p[0])
        assert p[1] == pytest.approx(exp_p[1])


def test_radar_response_bbox_geometry(radar_data, api):
    resp = api.simulate_get(
        '/radar?date=2023-05-08T11:30&lat=52&lon=7.6&distance=200000',
    )
    assert resp.json['geometry']['type'] == 'Polygon'
    expected_coords = [
        (4.54411, 53.61306),
        (5.04988, 50.17614),
        (10.37685, 50.31264),
        (10.41558, 53.76658),
    ]
    for p, exp_p in zip(resp.json['geometry']['coordinates'], expected_coords):
        assert p[0] == pytest.approx(exp_p[0])
        assert p[1] == pytest.approx(exp_p[1])
    assert resp.json['latlon_position']['x'] == pytest.approx(200.244)
    assert resp.json['latlon_position']['y'] == pytest.approx(200.088)


def test_radar_response_bbox_geometry_near_edge(radar_data, api):
    resp = api.simulate_get(
        '/radar?date=2023-05-08T11:30&lat=52&lon=2.6&distance=200000'
        '&fmt=plain',
    )
    assert resp.json['geometry']['type'] == 'Polygon'
    expected_coords = [
        (2.00507, 53.70663),
        (2.74712, 50.28395),
        (5.6003, 50.47017),
        (5.14662, 53.91603),
    ]
    for p, exp_p in zip(resp.json['geometry']['coordinates'], expected_coords):
        assert p[0] == pytest.approx(exp_p[0])
        assert p[1] == pytest.approx(exp_p[1])
    assert resp.json['latlon_position']['x'] == pytest.approx(14.326)
    assert resp.json['latlon_position']['y'] == pytest.approx(200.489)


def test_alerts_response(alerts_data, api):
    expected_ids = [
        '2.49.0.0.276.0.DWD.PVW.1687514160000.'
        '20999218-5d5e-4761-b271-6c243f695568',
        '2.49.0.0.276.0.DWD.PVW.1687470000000.'
        'fe90b61b-3755-4efb-8eda-b161251da9f7',
    ]
    # Query by lat/lon
    resp = api.simulate_get('/alerts?lat=51.55&lon=9.9')
    assert [
        alert['alert_id'] for alert in resp.json['alerts']
    ] == expected_ids
    assert resp.json['location'] == {
        'warn_cell_id': 803159016,
        'name': 'Stadt Göttingen',
        'name_short': 'Göttingen',
        'district': 'Göttingen',
        'state': 'Niedersachsen',
        'state_short': 'NI',
    }
    # Query by warn cell id
    resp = api.simulate_get('/alerts?warn_cell_id=803159016')
    assert len(resp.json['alerts']) == 2
    # Query by lat/lon, no alerts
    resp = api.simulate_get('/alerts?lat=52&lon=7.6')
    assert resp.status_code == 200
    assert not resp.json['alerts']
    assert resp.json['location']['name'] == 'Münster-Nord'
    # Query by warn_cell_id, no alerts
    resp = api.simulate_get('/alerts?warn_cell_id=705515101')
    assert not resp.json['alerts']
    # Query by lat/lon, outside of covered area
    resp = api.simulate_get('/alerts?lat=32&lon=7.6')
    assert resp.status_code == 404
    resp = api.simulate_get('/alerts?warn_cell_id=0')
    assert resp.status_code == 404


def test_status_response(api):
    resp = api.simulate_get('/')
    assert resp.status_code == 200
    assert resp.json['name'] == 'brightsky'
    assert resp.json['version'] == brightsky.__version__
    assert resp.json['status'] == 'ok'


def test_cors(synop_data, db):
    def _get_response(**kwargs):
        api = falcon.testing.TestClient(make_app())
        return api.simulate_get('/current_weather?lat=52&lon=7.6', **kwargs)

    example_com = 'http://example.com'
    brightsky_dev = 'https://brightsky.dev'

    with settings(CORS_ALLOW_ALL_ORIGINS=True):
        resp = _get_response(headers={'Origin': example_com})
        assert resp.headers['access-control-allow-origin'] == '*'

    with settings(CORS_ALLOWED_ORIGINS=[example_com, brightsky_dev]):
        resp = _get_response(headers={'Origin': example_com})
        assert resp.headers['access-control-allow-origin'] == example_com
        resp = _get_response(headers={'Origin': brightsky_dev})
        assert resp.headers['access-control-allow-origin'] == brightsky_dev

    with settings(CORS_ALLOWED_ORIGINS=[brightsky_dev]):
        resp = _get_response(headers={'Origin': example_com})
        assert 'access-control-allow-origin' not in resp.headers
        resp = _get_response(headers={'Origin': brightsky_dev})
        assert resp.headers['access-control-allow-origin'] == brightsky_dev
