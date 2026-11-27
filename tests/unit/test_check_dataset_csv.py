import csv
import urllib.request

ALLOWED = {"production", "staging", "development", ""}

def load():
    with open("pipeline/dataset.csv") as f:
        return list(csv.DictReader(f))

def test_environment_values_are_valid():
    bad = [r["dataset"] for r in load() if r["environment"] not in ALLOWED]
    assert not bad, f"Invalid environment for: {bad}"

def test_no_duplicate_datasets():
    names = [r["dataset"] for r in load()]
    assert len(names) == len(set(names))


def test_every_collection_dataset_has_a_row():
    url = "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv"
    spec = csv.DictReader(urllib.request.urlopen(url).read().decode().splitlines())
    expected = {r["dataset"] for r in spec if r.get("collection")}

    with open("pipeline/dataset.csv") as f:
        actual = {r["dataset"] for r in csv.DictReader(f)}

    assert expected == actual, f"missing: {expected - actual}, extra: {actual - expected}"