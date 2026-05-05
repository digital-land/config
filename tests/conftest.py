
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

@pytest.fixture(scope="session")
def prefix_aliases():
    query = (
        'select prefix, dataset from dataset '
        'where prefix in ("statistical-geography") '
        'order by dataset'
    )
    params = urlencode({"sql": query})
    url = f"{DATASETTE_BASE_URL}?{params}"

    with urlopen(url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))

    result = {}
    for row in payload.get("rows", []):
        if row and len(row) > 1 and row[0]:
            prefix = row[0]
            dataset = row[1]
            if prefix not in result:
                result[prefix] = []
            result[prefix].append(dataset)
    
    return result