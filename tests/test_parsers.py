import datetime

from dateutil.tz import tzutc

from brightsky.parsers import (
    CloudCoverObservationsParser, CurrentObservationsParser,
    DewPointObservationsParser, get_parser, MOSMIXParser,
    PrecipitationObservationsParser, PressureObservationsParser,
    SunshineObservationsParser, SYNOPParser, TemperatureObservationsParser,
    VisibilityObservationsParser, WindGustsObservationsParser,
    WindObservationsParser)

from .utils import is_subset, settings


def test_mosmix_parser(data_dir):
    p = MOSMIXParser()
    records = list(p.parse(data_dir / 'MOSMIX_S.kmz'))
    assert len(records) == 240
    assert records[0] == {
        'observation_type': 'forecast',
        'source': 'MOSMIX:2020-03-13T09:00:00.000Z',
        'lat': 74.52,
        'lon': 19.02,
        'height': 16.,
        'dwd_station_id': 'XXX',
        'wmo_station_id': '01028',
        'station_name': 'BJORNOYA',
        'timestamp': datetime.datetime(2020, 3, 13, 10, 0, tzinfo=tzutc()),
        'cloud_cover': 93.0,
        'dew_point': 257.25,
        'precipitation': 0.1,
        'precipitation_probability_6h': None,
        'pressure_msl': 99000.0,
        'sunshine': None,
        'temperature': 260.45,
        'visibility': 1700.0,
        'wind_direction': 330.0,
        'wind_speed': 8.75,
        'wind_gust_speed': None,
        'condition': 'snow',
    }
    assert records[-1] == {
        'observation_type': 'forecast',
        'source': 'MOSMIX:2020-03-13T09:00:00.000Z',
        'lat': 74.52,
        'lon': 19.02,
        'height': 16.,
        'dwd_station_id': 'XXX',
        'wmo_station_id': '01028',
        'station_name': 'BJORNOYA',
        'timestamp': datetime.datetime(2020, 3, 23, 9, 0, tzinfo=tzutc()),
        'cloud_cover': 76.,
        'dew_point': 265.35,
        'precipitation': None,
        'precipitation_probability_6h': None,
        'pressure_msl': 100630.0,
        'sunshine': None,
        'temperature': 267.15,
        'visibility': 11600.0,
        'wind_direction': 49.0,
        'wind_speed': 7.72,
        'wind_gust_speed': None,
        'condition': 'dry',
    }
    assert records[2]['precipitation_probability_6h'] == 49


def test_synop_parser(data_dir):
    p = SYNOPParser()
    records = list(p.parse(data_dir / 'synop.json.bz2'))
    assert len(records) == 3
    assert records[0] == {
        'observation_type': 'synop',
        'lat': 52.1344,
        'lon': 7.69685,
        'height': 47.8,
        'wmo_station_id': '10315',
        'dwd_station_id': '01766',
        'station_name': 'Muenster/Osnabrueck',
        'timestamp': datetime.datetime(2020, 6, 17, 9, 0, tzinfo=tzutc()),
        'cloud_cover': 88,
        'dew_point': 287.37,
        'pressure_msl': 101290,
        'relative_humidity': 66,
        'temperature': 294.05,
        'wind_direction_10': 30,
        'wind_speed_10': 2,
        'wind_gust_direction_10': None,
        'wind_gust_speed_10': None,
        'condition': 'dry',
    }
    assert records[1] == {
        'observation_type': 'synop',
        'lat': 52.1344,
        'lon': 7.69685,
        'height': 47.8,
        'wmo_station_id': '10315',
        'dwd_station_id': '01766',
        'station_name': 'Muenster/Osnabrueck',
        'timestamp': datetime.datetime(2020, 6, 17, 9, 0, tzinfo=tzutc()),
        'visibility': None,
        'sunshine_60': 2520,
        'precipitation_60': 0,
        'wind_direction_10': None,
        'wind_speed_10': None,
        'wind_gust_direction_10': None,
        'wind_gust_speed_10': None,
        'wind_gust_direction_60': None,
        'wind_gust_speed_60': 4.6,
        'wind_gust_direction_30': 340,
        'wind_gust_speed_30': 4.6,
    }
    assert records[2]['wmo_station_id'] == 'M031'
    assert records[2]['dwd_station_id'] == '05484'


def test_current_observation_parser(data_dir):
    p = CurrentObservationsParser()
    path = data_dir / 'observations_current.csv'
    records = list(p.parse(path, 10.1, 20.2, 30.3, 'Muenster'))
    assert len(records) == 25
    assert records[0] == {
        'observation_type': 'current',
        'lat': 10.1,
        'lon': 20.2,
        'height': 30.3,
        'dwd_station_id': 'YYY',
        'wmo_station_id': '01049',
        'station_name': 'Muenster',
        'timestamp': datetime.datetime(2020, 4, 6, 8, 0, tzinfo=tzutc()),
        'cloud_cover': None,
        'dew_point': 263.15,
        'precipitation': 0,
        'pressure_msl': 102310.,
        'relative_humidity': 59.,
        'sunshine': None,
        'temperature': 270.05,
        'visibility': None,
        'wind_direction': 140,
        'wind_speed': 3.9,
        'wind_gust_speed': 5.8,
        'condition': None,
    }
    assert records[15] == {
        'observation_type': 'current',
        'lat': 10.1,
        'lon': 20.2,
        'height': 30.3,
        'dwd_station_id': 'YYY',
        'wmo_station_id': '01049',
        'station_name': 'Muenster',
        'timestamp': datetime.datetime(2020, 4, 5, 17, 0, tzinfo=tzutc()),
        'cloud_cover': None,
        'dew_point': 270.05,
        'precipitation': 0.6,
        'pressure_msl': 101910.,
        'relative_humidity': 94.,
        'sunshine': None,
        'temperature': 270.95,
        'visibility': None,
        'wind_direction': 230,
        'wind_speed': 3.9,
        'wind_gust_speed': 7.2,
        'condition': None,
    }


def test_current_observation_parser_loads_station_data_from_db(db, data_dir):
    source = {
        'observation_type': 'forecast',
        'lat': 52.12,
        'lon': 7.62,
        'height': 5.,
        'wmo_station_id': '01049',
        'station_name': 'Münster',
    }
    db.insert('sources', [source])
    p = CurrentObservationsParser()
    record = next(p.parse(data_dir / 'observations_current.csv'))
    for field in ('lat', 'lon', 'height', 'station_name'):
        assert record[field] == source[field]


def test_observations_parser_parses_metadata(data_dir):
    p = WindObservationsParser()
    metadata = {
        'observation_type': 'historical',
        'source': (
            'Observations:Recent:produkt_ff_stunde_20180915_20200317_04911.txt'
        ),
        'lat': 48.8275,
        'lon': 12.5597,
        'height': 350.5,
        'dwd_station_id': '04911',
        'wmo_station_id': '10788',
        'station_name': 'Straubing',
    }
    for record in p.parse(data_dir / 'observations_recent_FF_akt.zip'):
        assert is_subset(metadata, record)


def test_observations_parser_handles_missing_values(data_dir):
    p = WindObservationsParser()
    records = list(p.parse(data_dir / 'observations_recent_FF_akt.zip'))
    assert records[5]['wind_direction'] == 90
    assert records[5]['wind_speed'] is None


def test_observations_parser_handles_ignored_values(data_dir):
    p = WindObservationsParser()
    p.ignored_values = {'wind_direction': ['80']}
    records = list(p.parse(path=data_dir / 'observations_recent_FF_akt.zip'))
    assert records[0]['wind_direction'] is None
    assert records[0]['wind_speed'] == 1.6


def test_observations_parser_handles_location_changes(data_dir):
    p = WindObservationsParser()
    path = data_dir / 'observations_recent_FF_location_change_akt.zip'
    records = list(p.parse(path))
    assert is_subset(
        {'lat': 48.8275, 'lon': 12.5597, 'height': 350.5}, records[0])
    assert is_subset(
        {'lat': 50.0, 'lon': 13.0, 'height': 345.0}, records[-1])


def test_observations_parser_skips_file_if_out_of_range(data_dir):
    p = PressureObservationsParser()
    path = data_dir / 'observations_19950901_20150817_hist.zip'
    assert not p.skip_path(path)
    with settings(
        MIN_DATE=datetime.datetime(2016, 1, 1, tzinfo=tzutc()),
    ):
        assert p.skip_path(path)
    with settings(
        MAX_DATE=datetime.datetime(1995, 1, 1, tzinfo=tzutc()),
    ):
        assert p.skip_path(path)


def test_observations_parser_skips_rows_if_before_cutoff(data_dir):
    p = WindObservationsParser()
    path = data_dir / 'observations_recent_FF_akt.zip'
    with settings(
        MIN_DATE=datetime.datetime(2019, 1, 1, tzinfo=tzutc()),
    ):
        records = list(p.parse(path))
        assert len(records) == 5
        assert records[0]['timestamp'] == datetime.datetime(
            2019, 4, 20, 21, tzinfo=tzutc())
    with settings(
        MAX_DATE=datetime.datetime(2019, 1, 1, tzinfo=tzutc()),
    ):
        records = list(p.parse(path))
        assert len(records) == 5
        assert records[-1]['timestamp'] == datetime.datetime(
            2018, 9, 15, 4, tzinfo=tzutc())


def _test_parser(
        cls, path, first, last, count=10, first_idx=0, last_idx=-1, **kwargs):
    records = list(cls().parse(path, **kwargs))
    first['timestamp'] = datetime.datetime.strptime(
        first['timestamp'], '%Y-%m-%d %H:%M').replace(tzinfo=tzutc())
    last['timestamp'] = datetime.datetime.strptime(
        last['timestamp'], '%Y-%m-%d %H:%M').replace(tzinfo=tzutc())
    assert len(records) == count
    assert is_subset(first, records[first_idx])
    assert is_subset(last, records[last_idx])


def test_cloud_cover_observations_parser(data_dir):
    _test_parser(
        CloudCoverObservationsParser,
        data_dir / 'observations_recent_N_akt.zip',
        {'timestamp': '2018-12-03 07:00', 'cloud_cover': 50},
        {'timestamp': '2019-11-20 00:00', 'cloud_cover': None},
    )


def test_dew_point_observations_parser(data_dir):
    _test_parser(
        DewPointObservationsParser,
        data_dir / 'observations_recent_TD_akt.zip',
        {'timestamp': '2018-12-03 00:00', 'dew_point': 284.55},
        {'timestamp': '2020-05-29 15:00', 'dew_point': 271.65},
    )


def test_temperature_observations_parser(data_dir):
    _test_parser(
        TemperatureObservationsParser,
        data_dir / 'observations_recent_TU_akt.zip',
        {'timestamp': '2018-09-15 00:00',
         'temperature': 286.85, 'relative_humidity': 96},
        {'timestamp': '2020-03-17 23:00',
         'temperature': 275.75, 'relative_humidity': 100},
    )


def test_precipitation_observations_parser(data_dir):
    _test_parser(
        PrecipitationObservationsParser,
        data_dir / 'observations_recent_RR_akt.zip',
        {'timestamp': '2018-09-22 20:00', 'precipitation': 0.0},
        {'timestamp': '2020-02-11 02:00', 'precipitation': 0.3},
    )


def test_visibility_observations_parser(data_dir):
    _test_parser(
        VisibilityObservationsParser,
        data_dir / 'observations_recent_VV_akt.zip',
        {'timestamp': '2018-12-03 00:00', 'visibility': 15000},
        {'timestamp': '2020-06-04 23:00', 'visibility': 30000},
    )


def test_wind_observations_parser(data_dir):
    _test_parser(
        WindObservationsParser,
        data_dir / 'observations_recent_FF_akt.zip',
        {'timestamp': '2018-09-15 00:00',
         'wind_speed': 1.6, 'wind_direction': 80},
        {'timestamp': '2020-03-17 23:00',
         'wind_speed': 1.5, 'wind_direction': 130},
    )


def test_wind_gusts_observations_parser(data_dir):
    _test_parser(
        WindGustsObservationsParser,
        data_dir / 'observations_recent_extrema_wind_akt.zip',
        {'timestamp': '2018-12-03 00:00',
         'wind_gust_speed': 6.3, 'wind_gust_direction': 210},
        {'timestamp': '2020-06-04 23:00',
         'wind_gust_speed': 6.2, 'wind_gust_direction': 270},
        meta_path=data_dir / 'observations_recent_extrema_wind_akt_meta.zip'
    )


def test_sunshine_observations_parser(data_dir):
    _test_parser(
        SunshineObservationsParser,
        data_dir / 'observations_recent_SD_akt.zip',
        {'timestamp': '2018-09-15 11:00', 'sunshine': 600.},
        {'timestamp': '2020-03-17 16:00', 'sunshine': 0.},
        first_idx=2,
    )


def test_pressure_observations_parser(data_dir):
    _test_parser(
        PressureObservationsParser,
        data_dir / 'observations_recent_P0_hist.zip',
        {'timestamp': '2018-09-15 00:00', 'pressure_msl': 102120},
        {'timestamp': '2020-03-17 23:00', 'pressure_msl': 103190},
    )


def test_pressure_observations_parser_approximates_pressure_msl(data_dir):
    p = PressureObservationsParser()
    records = list(p.parse(data_dir / 'observations_recent_P0_hist.zip'))
    # The actual reduced pressure deleted from the test observation file was
    # 1023.0 hPa
    assert records[4]['pressure_msl'] == 102260


def test_get_parser():
    synop_with_timestamp = (
        'Z__C_EDZW_20200617114802_bda01,synop_bufr_GER_999999_999999__MW_617'
        '.json.bz2')
    synop_latest = (
        'Z__C_EDZW_latest_bda01,synop_bufr_GER_999999_999999__MW_XXX.json.bz2')
    expected = {
        '10minutenwerte_extrema_wind_00427_akt.zip': (
            WindGustsObservationsParser),
        'stundenwerte_FF_00011_akt.zip': WindObservationsParser,
        'stundenwerte_FF_00090_akt.zip': WindObservationsParser,
        'stundenwerte_N_01766_akt.zip': CloudCoverObservationsParser,
        'stundenwerte_P0_00096_akt.zip': PressureObservationsParser,
        'stundenwerte_RR_00102_akt.zip': PrecipitationObservationsParser,
        'stundenwerte_SD_00125_akt.zip': SunshineObservationsParser,
        'stundenwerte_TD_01766.zip': DewPointObservationsParser,
        'stundenwerte_TU_00161_akt.zip': TemperatureObservationsParser,
        'stundenwerte_VV_00161_akt.zip': VisibilityObservationsParser,
        'MOSMIX_S_LATEST_240.kmz': MOSMIXParser,
        'K611_-BEOB.csv': CurrentObservationsParser,
        synop_with_timestamp: SYNOPParser,
        synop_latest: None,
    }
    for filename, expected_parser in expected.items():
        assert get_parser(filename) is expected_parser
