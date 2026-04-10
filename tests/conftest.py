
import os

import pytest
import urllib
from digital_land.specification import Specification



@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    Specification.download(specification_dir)
    return specification_dir