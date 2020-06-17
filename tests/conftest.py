import os
import time
from pathlib import Path

import pytest


@pytest.fixture
def data_dir():
    return Path(os.path.dirname(__file__)) / 'data'


def pytest_configure(config):
    # Dirty mock so we don't download the station list on every test run
    from brightsky.utils import _converter
    # Must contain all stations that we use in test data
    _converter.dwd_to_wmo = {
        'XXX': '01028',
        'YYY': '01049',
        '01766': '10315',
        '04911': '10788',
        '05484': 'M031',
    }
    _converter.wmo_to_dwd = dict(
        reversed(x) for x in _converter.dwd_to_wmo.items())
    _converter.last_update = time.time()
