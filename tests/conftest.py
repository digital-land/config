
import os

import pytest
from urllib.parse import urlencode
from urllib.request import urlopen
from digital_land.specification import Specification
import json


DATASETTE_BASE_URL = "https://datasette.planning.data.gov.uk/digital-land.json"

@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    Specification.download(specification_dir)
    return specification_dir

@pytest.fixture(scope="session")
def ended_organisations():
    query = (
        'select organisation from organisation '
        'where ("end_date" is not null and "end_date" != "") '
        'order by organisation desc'
    )
    params = urlencode({"sql": query})
    url = f"{DATASETTE_BASE_URL}?{params}"

    with urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return [row[0] for row in payload.get("rows", []) if row and row[0]]