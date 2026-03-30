
import os

import pytest
import urllib



@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    source_url = "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/"
    specification_csvs=[
        "attribution.csv",
        "licence.csv",
        "typology.csv",
        "theme.csv",
        "collection.csv",
        "dataset.csv",
        "dataset-field.csv",
        "field.csv",
        "datatype.csv",
        "prefix.csv",
        # deprecated .. THESE ARE NOT DEPRECCIATED YET STILL USED BY PACKAGES
        "pipeline.csv",
        "dataset-schema.csv",
        "provision-rule.csv",
        "schema.csv",
        "schema-field.csv",
    ]
    for csv_name in specification_csvs:
        urllib.request.urlretrieve(f"{source_url}{csv_name}", os.path.join(specification_dir, csv_name))
    return specification_dir