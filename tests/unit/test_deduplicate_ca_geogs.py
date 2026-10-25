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


HAA = "local-authority:HAA"
EHA = "local-authority:EHA"
AUTHORITATIVE_HAA = [(44016370, 44016384, HAA)]


def test_load_authoritative_entities_reads_ranges(tmp_path):
    path = tmp_path / "authoritative.csv"
    path.write_text(
        "dataset,entity-minimum,entity-maximum,organisation,notes\n"
        "conservation-area,10,20,local-authority:AAA,a note\n"
        "listed-building,1,2,local-authority:BBB,ignored dataset\n",
        encoding="utf-8",
    )

    assert dedupe.load_authoritative_entities(path) == [(10, 20, "local-authority:AAA")]


def test_load_authoritative_entities_missing_file_is_empty(tmp_path):
    assert dedupe.load_authoritative_entities(tmp_path / "missing.csv") == []


def test_repo_authoritative_list_parses_and_covers_havant_conservation_areas():
    ranges = dedupe.load_authoritative_entities()

    assert (44016370, 44016384, HAA) in ranges


def test_is_authoritative_requires_entity_in_range_and_matching_organisation():
    in_range = _check_row("1", "44016376", HE, HAA)
    wrong_org = _check_row("1", "44016376", HE, EHA)
    out_of_range = _check_row("1", "44016391", HE, HAA)

    assert dedupe.is_authoritative(in_range, AUTHORITATIVE_HAA)
    assert not dedupe.is_authoritative(wrong_org, AUTHORITATIVE_HAA)
    assert not dedupe.is_authoritative(out_of_range, AUTHORITATIVE_HAA)


def test_choose_redirect_target_prefers_authoritative_over_lowest_entity():
    candidates = [
        _check_row("1", "100", HE, "local-authority:LOW"),
        _check_row("1", "44016376", HE, HAA),
    ]

    chosen, reason = dedupe.choose_redirect_target(candidates, AUTHORITATIVE_HAA)

    assert chosen["entity_b"] == "44016376"
    assert reason == "authoritative"


def test_choose_redirect_target_falls_back_to_lowest_when_no_authoritative():
    candidates = [_check_row("1", "30", HE, HAA), _check_row("1", "20", HE, EHA)]

    chosen, reason = dedupe.choose_redirect_target(candidates)

    assert chosen["entity_b"] == "20"
    assert reason == "lowest"


def test_choose_redirect_target_lowest_of_several_authoritative_is_not_authoritative():
    candidates = [_check_row("1", "44016380", HE, HAA), _check_row("1", "44016375", HE, HAA)]

    chosen, reason = dedupe.choose_redirect_target(candidates, AUTHORITATIVE_HAA)

    assert chosen["entity_b"] == "44016375"
    assert reason == "lowest"


def test_choose_redirect_target_single_match_is_only():
    chosen, reason = dedupe.choose_redirect_target([_check_row("1", "5", HE, HAA)], AUTHORITATIVE_HAA)

    assert chosen["entity_b"] == "5"
    assert reason == "only"


def test_resolve_complete_matches_redirects_duplicates_to_authoritative_entity():
    rows = [
        _check_row("44017008", "44016398", HE, EHA),
        _check_row("44017008", "44016382", HE, HAA),
        _check_row("44016438", "44017008", EHA, HE),
    ]

    he_rows, duplicate_rows, flagged = dedupe.resolve_complete_matches(rows, AUTHORITATIVE_HAA)

    assert [(r["old-entity"], r["status"], r["entity"]) for r in he_rows] == [
        ("44017008", "301", "44016382")
    ]
    assert "authoritative of 3 complete matches" in he_rows[0]["notes"]
    assert sorted((r["old-entity"], r["entity"]) for r in duplicate_rows) == [
        ("44016398", "44016382"),
        ("44016438", "44016382"),
    ]
    assert all(r["status"] == "301" for r in duplicate_rows)
    assert flagged == []


def test_resolve_complete_matches_flags_but_does_not_redirect_duplicates_without_authority():
    rows = [
        _check_row("1", "30", HE, "local-authority:AAA"),
        _check_row("1", "20", HE, "local-authority:BBB"),
    ]

    he_rows, duplicate_rows, flagged = dedupe.resolve_complete_matches(rows, AUTHORITATIVE_HAA)

    assert he_rows[0]["entity"] == "20"
    assert duplicate_rows == []
    assert len(flagged) == 1
    assert flagged[0]["old-entity"] == "1"
    assert flagged[0]["chosen"] == "20"
    assert flagged[0]["others"] == [("30", "local-authority:AAA")]


def test_resolve_complete_matches_does_not_flag_single_or_authoritative_matches():
    rows = [
        _check_row("1", "5", HE, EHA),
        _check_row("2", "44016376", HE, HAA),
        _check_row("2", "44016391", HE, EHA),
    ]

    _, _, flagged = dedupe.resolve_complete_matches(rows, AUTHORITATIVE_HAA)

    assert flagged == []


def test_resolve_complete_matches_shared_duplicate_is_redirected_once():
    rows = [
        _check_row("1", "44016376", HE, HAA),
        _check_row("1", "44016391", HE, EHA),
        _check_row("2", "44016375", HE, HAA),
        _check_row("2", "44016391", HE, EHA),
    ]

    _, duplicate_rows, _ = dedupe.resolve_complete_matches(rows, AUTHORITATIVE_HAA)

    assert [(r["old-entity"], r["entity"]) for r in duplicate_rows] == [("44016391", "44016376")]


def test_filter_authoritative_duplicates_keeps_duplicate_that_is_a_redirect_target():
    old_entity = [{"old-entity": "44009285", "status": "301", "entity": "44016391"}]
    duplicates = [_new_match("44016391", entity="44016376")]

    assert dedupe.filter_authoritative_duplicates(old_entity, duplicates, []) == duplicates


def test_filter_authoritative_duplicates_skips_existing_source_and_new_source():
    old_entity = [{"old-entity": "44016391", "status": "301", "entity": "99"}]
    duplicates = [_new_match("44016391", entity="44016376"), _new_match("44016392", entity="44016377")]

    result = dedupe.filter_authoritative_duplicates(old_entity, duplicates, [_new_match("44016392", entity="1")])

    assert result == []


def test_filter_authoritative_duplicates_skips_loops():
    old_entity = [{"old-entity": "44016376", "status": "301", "entity": "44016391"}]
    duplicates = [_new_match("44016391", entity="44016376")]

    assert dedupe.filter_authoritative_duplicates(old_entity, duplicates, []) == []


def test_report_flagged_matches_prints_warning_and_writes_summary(tmp_path, monkeypatch, capsys):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    flagged = [{
        "old-entity": "44017008",
        "chosen": "44016382",
        "chosen_org": HAA,
        "others": [("44016398", EHA)],
    }]

    dedupe.report_flagged_matches(flagged)

    out = capsys.readouterr().out
    assert "::warning" in out
    assert "44017008" in out
    text = summary.read_text(encoding="utf-8")
    assert "44017008" in text and "44016398" in text


def test_report_flagged_matches_is_silent_when_nothing_flagged(tmp_path, monkeypatch, capsys):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    dedupe.report_flagged_matches([])

    assert capsys.readouterr().out == ""
    assert not summary.exists()
