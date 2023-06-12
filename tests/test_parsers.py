import datetime

import numpy as np
from dateutil.tz import tzutc
from freezegun import freeze_time
from isal import isal_zlib as zlib

from brightsky.parsers import (
    CloudCoverObservationsParser,
    CurrentObservationsParser,
    DewPointObservationsParser,
    get_parser,
    MOSMIXParser,
    PrecipitationObservationsParser,
    PressureObservationsParser,
    RADOLANParser,
    SolarRadiationObservationsParser,
    SunshineObservationsParser,
    SYNOPParser,
    TemperatureObservationsParser,
    VisibilityObservationsParser,
    WindGustsObservationsParser,
    WindObservationsParser,
)

from .utils import settings


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


def test_radolan_parser(data_dir):
    p = RADOLANParser()
    records = list(p.parse(data_dir / 'DE1200_RV2305081330.tar.bz2'))
    assert len(records) == 1
    data = np.frombuffer(
        zlib.decompress(records[0]['precipitation_5']),
        dtype='i2',
    )
    assert len(data) == 1200 * 1100
    assert sum(data) == 564030
    assert len(np.where(data < 4096)[0]) == len(data)
    assert data.reshape((1200, 1100))[1117:1122, 334:339].tolist() == [
        [3, 5, 2, 1, 3],
        [2, 3, 3, 0, 0],
        [3, 4, 1, 0, 3],
        [0, 8, 0, 0, 0],
        [0, 0, 0, 0, 0],
    ]


@freeze_time('2023-06-11')
def test_solar_radiation_parser_skips_today(data_dir):
    p = SolarRadiationObservationsParser()
    records = list(p.parse(
        data_dir / '10minutenwerte_SOLAR_01766_akt.zip',
        meta_path=data_dir / 'Meta_Daten_zehn_min_sd_01766.zip'
    ))
    assert records[-1]['timestamp'] == datetime.datetime(
        2023, 6, 10, 23, tzinfo=tzutc())


def test_get_parser():
    synop_with_timestamp = (
        'Z__C_EDZW_20200617114802_bda01,synop_bufr_GER_999999_999999__MW_617'
        '.json.bz2')
    synop_latest = (
        'Z__C_EDZW_latest_bda01,synop_bufr_GER_999999_999999__MW_XXX.json.bz2')
    expected = {
        '10minutenwerte_extrema_wind_00427_akt.zip': (
            WindGustsObservationsParser),
        '10minutenwerte_SOLAR_01766_now.zip': SolarRadiationObservationsParser,
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
        'DE1200_RV2305081330.tar.bz2': RADOLANParser,
        'K611_-BEOB.csv': CurrentObservationsParser,
        synop_with_timestamp: SYNOPParser,
        synop_latest: None,
    }
    for filename, expected_parser in expected.items():
        assert get_parser(filename) is expected_parser
