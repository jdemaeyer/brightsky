import os
from pathlib import Path

import pytest


@pytest.fixture
def data_dir():
    return Path(os.path.dirname(__file__)) / 'data'
