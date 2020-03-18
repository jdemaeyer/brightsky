import datetime
import os

import pytest
from dateutil.tz import tzutc

from brightsky.parsers import MOSMIXParser


@pytest.fixture
def data_dir():
    from pathlib import Path
    return Path(os.path.dirname(__file__)) / 'data'


def test_mosmix_parser(data_dir):
    p = MOSMIXParser(path=data_dir / 'MOSMIX_S.kmz')
    records = list(p.parse())
    assert len(records) == 240
    assert records[0] == {
        'type': 'forecast',
        'source': 'MOSMIX:2020-03-13T09:00:00.000Z',
        'station_id': '01028',
        'lat': 19.02,
        'lon': 74.52,
        'timestamp': datetime.datetime(2020, 3, 13, 10, 0, tzinfo=tzutc()),
        'temperature': 260.45,
        'wind_direction': 330.0,
        'wind_speed': 8.75,
        'precipitation': 0.1,
        'sunshine': None,
        'pressure': 99000.0,
    }
    assert records[-1] == {
        'type': 'forecast',
        'source': 'MOSMIX:2020-03-13T09:00:00.000Z',
        'station_id': '01028',
        'lat': 19.02,
        'lon': 74.52,
        'timestamp': datetime.datetime(2020, 3, 23, 9, 0, tzinfo=tzutc()),
        'temperature': 267.15,
        'wind_direction': 49.0,
        'wind_speed': 7.72,
        'precipitation': None,
        'sunshine': None,
        'pressure': 100630.0,
    }
