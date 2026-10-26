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
HAA = "local-authority:HAA"
EHA = "local-authority:EHA"


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


def _he_pair(he_entity, lpa_entity, lpa_org):
    """Historic England as entity_a, the LPA entity as entity_b."""
    return _check_row(he_entity, lpa_entity, HE, lpa_org)


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


# ---------------------------------------------------------------- historic_england_pairs

def test_historic_england_pairs_orients_he_as_entity_a():
    rows = [_check_row("2", "1", HAA, HE)]

    pairs = dedupe.historic_england_pairs(rows, "complete_match")

    assert len(pairs) == 1
    assert pairs[0]["entity_a"] == "1"
    assert pairs[0]["entity_b"] == "2"
    assert pairs[0]["lookup_org_a"] == HE
    assert pairs[0]["lookup_org_b"] == HAA


def test_historic_england_pairs_counts_pair_reported_both_ways_once():
    rows = [
        _check_row("1", "2", HE, HAA),
        _check_row("2", "1", HAA, HE),
    ]

    assert len(dedupe.historic_england_pairs(rows, "complete_match")) == 1


def test_historic_england_pairs_ignores_he_to_he_and_non_he_pairs():
    rows = [
        _check_row("1", "2", HE, HE),
        _check_row("3", "4", HAA, EHA),
    ]

    assert dedupe.historic_england_pairs(rows, "complete_match") == []


# ---------------------------------------------------------------- resolve_complete_matches

def test_resolve_single_match_is_a_plain_redirect():
    rows = [_he_pair("5", "50", HAA)]

    he_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert [(r["old-entity"], r["status"], r["entity"]) for r in he_rows] == [("5", "301", "50")]
    assert he_rows[0]["notes"] == "Redirect old entity complete match to LPA entity: local-authority:HAA"
    assert ambiguous == []


def test_resolve_handles_pairs_reported_with_he_as_entity_b():
    rows = [_check_row("44016370", "44017000", HAA, HE)]

    he_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert [(r["old-entity"], r["entity"]) for r in he_rows] == [("44017000", "44016370")]
    assert ambiguous == []


def test_resolve_writes_nothing_for_an_entity_with_several_matches_but_resolves_the_others():
    rows = [
        _he_pair("44017000", "44016370", HAA),
        _he_pair("44017008", "44016398", EHA),
        _he_pair("44017008", "44016382", HAA),
        _he_pair("44017008", "44016438", EHA),
    ]

    he_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert [r["old-entity"] for r in he_rows] == ["44017000"]
    assert [a["old-entity"] for a in ambiguous] == ["44017008"]
    assert ambiguous[0]["matches"] == [
        ("44016382", HAA),
        ("44016398", EHA),
        ("44016438", EHA),
    ]


def test_resolve_counts_a_pair_reported_both_ways_as_one_match():
    rows = [_he_pair("1", "10", HAA), _check_row("10", "1", HAA, HE)]

    he_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert [(r["old-entity"], r["entity"]) for r in he_rows] == [("1", "10")]
    assert ambiguous == []


def test_resolve_only_redirects_historic_england_entities():
    rows = [
        _he_pair("1", "10", HAA),
        _he_pair("2", "10", HAA),
        _check_row("10", "20", HAA, EHA),
    ]

    he_rows, _ = dedupe.resolve_complete_matches(rows)

    assert sorted(r["old-entity"] for r in he_rows) == ["1", "2"]
    assert {r["status"] for r in he_rows} == {"301"}


def test_extract_complete_matches_returns_only_resolved_historic_england_rows():
    rows = [
        _he_pair("1", "10", HAA),
        _he_pair("2", "30", HAA),
        _he_pair("2", "40", EHA),
    ]

    assert [r["old-entity"] for r in dedupe.extract_complete_matches(rows)] == ["1"]


# ---------------------------------------------------------------- single matches and uniqueness

def test_drop_superseded_single_matches_prefers_complete_match():
    complete = [_new_match("1", entity="10")]
    single = [
        _new_match("1", entity="20"),
        _new_match("2", status="410", notes="Matches with multiple entities"),
    ]

    result = dedupe.drop_superseded_single_matches(complete, single)

    assert [m["old-entity"] for m in result] == ["2"]


def test_drop_superseded_single_matches_also_skips_ambiguous_entities():
    single = [_new_match("7", entity="70"), _new_match("8", entity="80")]

    result = dedupe.drop_superseded_single_matches([], single, {"7"})

    assert [m["old-entity"] for m in result] == ["8"]


def test_check_old_entities_unique_accepts_unique_rows():
    dedupe.check_old_entities_unique([_new_match("1", entity="10"), _new_match("2", entity="20")])


def test_check_old_entities_unique_rejects_duplicates():
    rows = [_new_match("1", entity="10"), _new_match("1", entity="20"), _new_match("2", entity="30")]

    with pytest.raises(ValueError, match="1"):
        dedupe.check_old_entities_unique(rows)


# ---------------------------------------------------------------- reporting

AMBIGUOUS = [{
    "old-entity": "44017008",
    "matches": [("44016382", HAA), ("44016398", EHA), ("44016438", EHA)],
}]


def test_report_ambiguous_matches_prints_an_error_and_writes_the_summary(tmp_path, monkeypatch, capsys):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    dedupe.report_ambiguous_matches(AMBIGUOUS)

    out = capsys.readouterr().out
    assert "::error" in out
    assert "44017008" in out
    assert "by hand" in out
    text = summary.read_text(encoding="utf-8")
    assert "44017008" in text and "44016398 (local-authority:EHA)" in text


def test_report_ambiguous_matches_is_silent_when_nothing_is_ambiguous(tmp_path, monkeypatch, capsys):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    dedupe.report_ambiguous_matches([])

    assert capsys.readouterr().out == ""
    assert not summary.exists()


# ---------------------------------------------------------------- main exit code and written data

def _run_main(monkeypatch, tmp_path, checks, existing_rows=()):
    import csv

    path = tmp_path / "old-entity.csv"
    fieldnames = ["old-entity", "status", "entity", "notes", "end-date", "entry-date", "start-date"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_rows)

    monkeypatch.setattr(dedupe, "stream_checks_data", lambda: checks)
    monkeypatch.setattr(dedupe, "OLD_ENTITY_PATH", path)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)

    code = dedupe.main()
    with open(path, newline="", encoding="utf-8") as f:
        return code, list(csv.DictReader(f))


def test_main_returns_zero_when_everything_is_resolved(monkeypatch, tmp_path):
    checks = [_he_pair("44017000", "44016370", HAA)]

    code, rows = _run_main(monkeypatch, tmp_path, checks)

    assert code == 0
    assert [(r["old-entity"], r["entity"]) for r in rows] == [("44017000", "44016370")]


def test_main_writes_the_safe_rows_then_fails_for_an_ambiguous_entity(monkeypatch, tmp_path):
    checks = [
        _he_pair("44017000", "44016370", HAA),
        _he_pair("44017008", "44016382", HAA),
        _he_pair("44017008", "44016398", EHA),
        _he_pair("44017008", "44016438", EHA),
    ]

    code, rows = _run_main(monkeypatch, tmp_path, checks)

    assert code == 1
    old_entities = [r["old-entity"] for r in rows]
    assert old_entities == ["44017000"]
    assert len(old_entities) == len(set(old_entities))


def test_main_is_a_no_op_on_a_second_run(monkeypatch, tmp_path):
    checks = [_he_pair("44017000", "44016370", HAA)]
    _, first = _run_main(monkeypatch, tmp_path, checks)

    _, second = _run_main(monkeypatch, tmp_path, checks, existing_rows=first)

    assert second == first


def test_main_leaves_a_manually_resolved_entity_alone(monkeypatch, tmp_path):
    checks = [
        _he_pair("44017008", "44016382", HAA),
        _he_pair("44017008", "44016398", EHA),
    ]
    manual = [_new_match("44017008", entity="44016382", notes="Manual decision")]

    code, rows = _run_main(monkeypatch, tmp_path, checks, existing_rows=manual)

    # the entity is already a source, so it is skipped by the conflict filter, but it is still
    # reported until the report stops listing it (its Historic England entity is retired)
    assert [r["old-entity"] for r in rows] == ["44017008"]
    assert rows[0]["notes"] == "Manual decision"
    assert code == 1
