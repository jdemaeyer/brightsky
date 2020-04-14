import datetime

from dateutil.tz import tzutc

from brightsky.parsers import (
    CurrentObservationsParser, get_parser, MOSMIXParser,
    PrecipitationObservationsParser, PressureObservationsParser,
    SunshineObservationsParser, TemperatureObservationsParser,
    WindObservationsParser)

from .utils import is_subset, overridden_settings


def test_mosmix_parser(data_dir):
    p = MOSMIXParser(path=data_dir / 'MOSMIX_S.kmz')
    records = list(p.parse())
    assert len(records) == 240
    assert records[0] == {
        'observation_type': 'forecast',
        'source': 'MOSMIX:2020-03-13T09:00:00.000Z',
        'station_id': '01028',
        'lat': 19.02,
        'lon': 74.52,
        'height': 16.,
        'timestamp': datetime.datetime(2020, 3, 13, 10, 0, tzinfo=tzutc()),
        'temperature': 260.45,
        'wind_direction': 330.0,
        'wind_speed': 8.75,
        'precipitation': 0.1,
        'sunshine': None,
        'pressure_msl': 99000.0,
    }
    assert records[-1] == {
        'observation_type': 'forecast',
        'source': 'MOSMIX:2020-03-13T09:00:00.000Z',
        'station_id': '01028',
        'lat': 19.02,
        'lon': 74.52,
        'height': 16.,
        'timestamp': datetime.datetime(2020, 3, 23, 9, 0, tzinfo=tzutc()),
        'temperature': 267.15,
        'wind_direction': 49.0,
        'wind_speed': 7.72,
        'precipitation': None,
        'sunshine': None,
        'pressure_msl': 100630.0,
    }


def test_current_observation_parser(data_dir):
    p = CurrentObservationsParser(path=data_dir / 'observations_current.csv')
    records = list(p.parse(10.1, 20.2, 30.3))
    assert len(records) == 25
    assert records[0] == {
        'observation_type': 'current',
        'station_id': '01049',
        'lat': 10.1,
        'lon': 20.2,
        'height': 30.3,
        'timestamp': datetime.datetime(2020, 4, 6, 8, 0, tzinfo=tzutc()),
        'temperature': 270.05,
        'wind_direction': 140,
        'wind_speed': 3.9,
        'precipitation': 0,
        'pressure_msl': 102310.,
        'sunshine': None,
    }
    assert records[15] == {
        'observation_type': 'current',
        'station_id': '01049',
        'lat': 10.1,
        'lon': 20.2,
        'height': 30.3,
        'timestamp': datetime.datetime(2020, 4, 5, 17, 0, tzinfo=tzutc()),
        'temperature': 270.95,
        'wind_direction': 230,
        'wind_speed': 3.9,
        'precipitation': 0.6,
        'pressure_msl': 101910.,
        'sunshine': None,
    }


def test_observations_parser_parses_metadata(data_dir):
    p = WindObservationsParser(
        path=data_dir / 'observations_recent_FF_akt.zip')
    metadata = {
        'observation_type': 'recent',
        'source': (
            'Observations:Recent:produkt_ff_stunde_20180915_20200317_04911.txt'
        ),
        'station_id': '04911',
        'lat': 12.5597,
        'lon': 48.8275,
        'height': 350.5,
    }
    for record in p.parse():
        assert is_subset(metadata, record)


def test_observations_parser_parses_historical_observation_type(data_dir):
    p = PressureObservationsParser(
        path=data_dir / 'observations_recent_P0_hist.zip')
    for record in p.parse():
        assert record['observation_type'] == 'historical'


def test_observations_parser_handles_missing_values(data_dir):
    p = WindObservationsParser(
        path=data_dir / 'observations_recent_FF_akt.zip')
    records = list(p.parse())
    assert records[5]['wind_direction'] == 90
    assert records[5]['wind_speed'] is None


def test_observations_parser_handles_location_changes(data_dir):
    p = WindObservationsParser(
        path=data_dir / 'observations_recent_FF_location_change_akt.zip')
    records = list(p.parse())
    assert is_subset(
        {'lat': 12.5597, 'lon': 48.8275, 'height': 350.5}, records[0])
    assert is_subset(
        {'lat': 13.0, 'lon': 50.0, 'height': 345.0}, records[-1])


def test_observations_parser_skips_file_if_ends_before_cutoff(data_dir):
    p = PressureObservationsParser(
        path=data_dir / 'observations_19950901_20050817_hist.zip')
    assert p.should_skip()


def test_observations_parser_skips_rows_if_before_cutoff(data_dir):
    p = WindObservationsParser(
        path=data_dir / 'observations_recent_FF_akt.zip')
    with overridden_settings(
        DATE_CUTOFF=datetime.datetime(2019, 1, 1, tzinfo=tzutc()),
    ):
        records = list(p.parse())
        assert len(records) == 5


def _test_parser(cls, path, first, last, count=10, first_idx=0, last_idx=-1):
    p = cls(path=path)
    records = list(p.parse())
    first['timestamp'] = datetime.datetime.strptime(
        first['timestamp'], '%Y-%m-%d %H:%M').replace(tzinfo=tzutc())
    last['timestamp'] = datetime.datetime.strptime(
        last['timestamp'], '%Y-%m-%d %H:%M').replace(tzinfo=tzutc())
    assert len(records) == count
    assert is_subset(first, records[first_idx])
    assert is_subset(last, records[last_idx])


def test_temperature_observations_parser(data_dir):
    _test_parser(
        TemperatureObservationsParser,
        data_dir / 'observations_recent_TU_akt.zip',
        {'timestamp': '2018-09-15 00:00', 'temperature': 286.85},
        {'timestamp': '2020-03-17 23:00', 'temperature': 275.75},
    )


def test_precipitation_observations_parser(data_dir):
    _test_parser(
        PrecipitationObservationsParser,
        data_dir / 'observations_recent_RR_akt.zip',
        {'timestamp': '2018-09-22 20:00', 'precipitation': 0.0},
        {'timestamp': '2020-02-11 02:00', 'precipitation': 0.3},
    )


def test_wind_observations_parser(data_dir):
    _test_parser(
        WindObservationsParser,
        data_dir / 'observations_recent_FF_akt.zip',
        {'timestamp': '2018-09-15 00:00',
         'wind_speed': 1.6, 'wind_direction': 80.0},
        {'timestamp': '2020-03-17 23:00',
         'wind_speed': 1.5, 'wind_direction': 130.0},
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
        {'timestamp': '2018-09-15 00:00', 'pressure_msl': 98090.},
        {'timestamp': '2020-03-17 23:00', 'pressure_msl': 98980.},
    )


def test_get_parser():
    expected = {
        'stundenwerte_FF_00011_akt.zip': WindObservationsParser,
        'stundenwerte_FF_00090_akt.zip': WindObservationsParser,
        'stundenwerte_P0_00096_akt.zip': PressureObservationsParser,
        'stundenwerte_RR_00102_akt.zip': PrecipitationObservationsParser,
        'stundenwerte_SD_00125_akt.zip': SunshineObservationsParser,
        'stundenwerte_TU_00161_akt.zip': TemperatureObservationsParser,
        'MOSMIX_S_LATEST_240.kmz': MOSMIXParser,
        'K611_-BEOB.csv': CurrentObservationsParser,
    }
    for filename, expected_parser in expected.items():
        assert get_parser(filename) is expected_parser
