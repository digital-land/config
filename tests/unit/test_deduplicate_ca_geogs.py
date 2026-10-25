import importlib.util
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "scripts" / "deduplicate-ca-geogs.py"
)
_spec = importlib.util.spec_from_file_location("deduplicate_ca_geogs", SCRIPT_PATH)
dedupe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dedupe)

HE = dedupe.HISTORIC_ENGLAND_ORG


def _check_row(entity_a, entity_b, org_a, org_b, message="complete_match", in_odp="True"):
    return {
        "message": message,
        "dataset": "conservation-area",
        "entity_a": entity_a,
        "entity_b": entity_b,
        "entity_a_name": f"name {entity_a}",
        "entity_b_name": f"name {entity_b}",
        "lookup_org_a": org_a,
        "lookup_org_b": org_b,
        "in_odp": in_odp,
    }


def _new_match(old_entity, status="301", entity="", notes="note", entry_date="2026-10-23"):
    return {
        "old-entity": old_entity,
        "status": status,
        "entity": entity,
        "end-date": "",
        "notes": notes,
        "entry-date": entry_date,
        "start-date": "",
    }


def test_historic_england_pairs_orients_he_as_entity_a():
    rows = [_check_row("2", "1", "local-authority:HAA", HE)]

    pairs = dedupe.historic_england_pairs(rows, "complete_match")

    assert len(pairs) == 1
    assert pairs[0]["entity_a"] == "1"
    assert pairs[0]["entity_b"] == "2"
    assert pairs[0]["lookup_org_a"] == HE
    assert pairs[0]["lookup_org_b"] == "local-authority:HAA"


def test_historic_england_pairs_counts_pair_reported_both_ways_once():
    rows = [
        _check_row("1", "2", HE, "local-authority:HAA"),
        _check_row("2", "1", "local-authority:HAA", HE),
    ]

    assert len(dedupe.historic_england_pairs(rows, "complete_match")) == 1


def test_historic_england_pairs_ignores_he_to_he_and_non_he_pairs():
    rows = [
        _check_row("1", "2", HE, HE),
        _check_row("3", "4", "local-authority:HAA", "local-authority:EHA"),
    ]

    assert dedupe.historic_england_pairs(rows, "complete_match") == []


def test_extract_complete_matches_redirects_to_lowest_entity_when_multiple_matches():
    rows = [
        _check_row("44017008", "44016398", HE, "local-authority:EHA"),
        _check_row("44017008", "44016382", HE, "local-authority:HAA"),
        _check_row("44016438", "44017008", "local-authority:EHA", HE),
    ]

    matches = dedupe.extract_complete_matches(rows)

    assert len(matches) == 1
    assert matches[0]["old-entity"] == "44017008"
    assert matches[0]["status"] == "301"
    assert matches[0]["entity"] == "44016382"
    assert matches[0]["notes"].startswith(
        "Redirect old entity complete match to LPA entity: local-authority:HAA"
    )
    assert "1 of 3 complete matches" in matches[0]["notes"]


def test_extract_complete_matches_compares_entity_numbers_numerically():
    rows = [
        _check_row("1", "100", HE, "local-authority:AAA"),
        _check_row("1", "99", HE, "local-authority:BBB"),
    ]

    assert dedupe.extract_complete_matches(rows)[0]["entity"] == "99"


def test_extract_complete_matches_choice_does_not_depend_on_row_order():
    rows = [
        _check_row("1", "30", HE, "local-authority:AAA"),
        _check_row("1", "10", HE, "local-authority:BBB"),
        _check_row("1", "20", HE, "local-authority:CCC"),
    ]

    forward = dedupe.extract_complete_matches(rows)
    backward = dedupe.extract_complete_matches(list(reversed(rows)))

    assert forward == backward
    assert forward[0]["entity"] == "10"


def test_extract_complete_matches_single_match_keeps_plain_note():
    rows = [_check_row("5", "50", HE, "local-authority:HAA")]

    matches = dedupe.extract_complete_matches(rows)

    assert matches[0]["notes"] == "Redirect old entity complete match to LPA entity: local-authority:HAA"


def test_extract_complete_matches_never_ends_an_entity():
    rows = [
        _check_row("1", "10", HE, "local-authority:HAA"),
        _check_row("1", "20", HE, "local-authority:EHA"),
        _check_row("2", "30", HE, "local-authority:HAA"),
    ]

    matches = dedupe.extract_complete_matches(rows)

    assert {m["status"] for m in matches} == {"301"}
    assert all(m["entity"] for m in matches)
    assert sorted(m["old-entity"] for m in matches) == ["1", "2"]


def test_drop_superseded_single_matches_prefers_complete_match():
    complete = [_new_match("1", entity="10")]
    single = [
        _new_match("1", entity="20"),
        _new_match("2", status="410", notes="Matches with multiple entities"),
    ]

    result = dedupe.drop_superseded_single_matches(complete, single)

    assert [m["old-entity"] for m in result] == ["2"]


def test_check_old_entities_unique_accepts_unique_rows():
    dedupe.check_old_entities_unique([_new_match("1", entity="10"), _new_match("2", entity="20")])


def test_check_old_entities_unique_rejects_duplicates():
    rows = [_new_match("1", entity="10"), _new_match("1", entity="20"), _new_match("2", entity="30")]

    with pytest.raises(ValueError, match="1"):
        dedupe.check_old_entities_unique(rows)


def test_complete_and_single_matches_combined_have_unique_old_entities():
    complete = dedupe.extract_complete_matches(
        [
            _check_row("1", "10", HE, "local-authority:HAA"),
            _check_row("20", "1", "local-authority:EHA", HE),
        ]
    )
    single = [_new_match("1", entity="30"), _new_match("7", entity="70")]

    combined = complete + dedupe.drop_superseded_single_matches(complete, single)

    dedupe.check_old_entities_unique(combined)
    assert sorted(m["old-entity"] for m in combined) == ["1", "7"]
