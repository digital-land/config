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


def _check_row(entity_a, entity_b, org_a, org_b, message="complete_match", in_odp="True",
               a_out_of_bounds="", b_out_of_bounds=""):
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
        "entity_a_out_of_bounds": a_out_of_bounds,
        "entity_b_out_of_bounds": b_out_of_bounds,
    }


def _he_pair(he_entity, lpa_entity, lpa_org, out_of_bounds):
    """Historic England as entity_a, the LPA entity as entity_b."""
    return _check_row(he_entity, lpa_entity, HE, lpa_org, b_out_of_bounds=out_of_bounds)


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
    rows = [_check_row("2", "1", "local-authority:HAA", HE)]

    pairs = dedupe.historic_england_pairs(rows, "complete_match")

    assert len(pairs) == 1
    assert pairs[0]["entity_a"] == "1"
    assert pairs[0]["entity_b"] == "2"
    assert pairs[0]["lookup_org_a"] == HE
    assert pairs[0]["lookup_org_b"] == "local-authority:HAA"


def test_historic_england_pairs_swaps_out_of_bounds_flags_with_the_entities():
    rows = [_check_row("44016391", "44017000", EHA, HE, a_out_of_bounds="True", b_out_of_bounds="")]

    pair = dedupe.historic_england_pairs(rows, "complete_match")[0]

    assert pair["entity_b"] == "44016391"
    assert pair["entity_b_out_of_bounds"] == "True"


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


# ---------------------------------------------------------------- parse_flag

@pytest.mark.parametrize("value,expected", [
    ("True", True), ("true", True), (" TRUE ", True),
    ("False", False), ("false", False),
    ("", None), (None, None), ("nan", None), ("<NA>", None), ("maybe", None),
])
def test_parse_flag(value, expected):
    assert dedupe.parse_flag(value) is expected


# ---------------------------------------------------------------- choose_redirect_target

def test_choose_single_match_is_only_whatever_the_flag():
    for flag in ("True", "False", ""):
        chosen, reason = dedupe.choose_redirect_target([_he_pair("1", "5", HAA, flag)])

        assert chosen["entity_b"] == "5"
        assert reason == "only"


def test_choose_picks_the_one_candidate_inside_its_boundary():
    candidates = [_he_pair("1", "44016391", EHA, "True"), _he_pair("1", "44016376", HAA, "False")]

    chosen, reason = dedupe.choose_redirect_target(candidates)

    assert chosen["entity_b"] == "44016376"
    assert reason == "in-bounds"


def test_choose_does_not_depend_on_row_order_or_entity_number():
    in_bounds_has_higher_number = [_he_pair("1", "10", EHA, "True"), _he_pair("1", "99", HAA, "False")]

    forward, _ = dedupe.choose_redirect_target(in_bounds_has_higher_number)
    backward, _ = dedupe.choose_redirect_target(list(reversed(in_bounds_has_higher_number)))

    assert forward["entity_b"] == backward["entity_b"] == "99"


def test_choose_is_ambiguous_when_several_candidates_are_inside():
    # the Sir George Staunton case: copies that only touch the boundary are not flagged
    candidates = [
        _he_pair("44017008", "44016382", HAA, "False"),
        _he_pair("44017008", "44016398", EHA, "False"),
        _he_pair("44017008", "44016438", EHA, "False"),
    ]

    chosen, reason = dedupe.choose_redirect_target(candidates)

    assert chosen is None
    assert reason == "ambiguous"


def test_choose_is_ambiguous_when_all_candidates_are_outside():
    candidates = [_he_pair("1", "10", HAA, "True"), _he_pair("1", "20", EHA, "True")]

    assert dedupe.choose_redirect_target(candidates) == (None, "ambiguous")


def test_choose_is_ambiguous_when_any_flag_is_unknown():
    # one known inside and one unknown: the unknown one could also be inside
    candidates = [_he_pair("1", "10", HAA, "False"), _he_pair("1", "20", EHA, "")]

    assert dedupe.choose_redirect_target(candidates) == (None, "ambiguous")


def test_choose_is_ambiguous_when_the_column_is_missing_from_the_report():
    rows = [_he_pair("1", "10", HAA, ""), _he_pair("1", "20", EHA, "")]
    for row in rows:
        del row["entity_b_out_of_bounds"]

    assert dedupe.choose_redirect_target(rows) == (None, "ambiguous")


# ---------------------------------------------------------------- resolve_complete_matches

def test_resolve_redirects_he_entity_and_its_out_of_bounds_copy_to_the_inside_one():
    rows = [
        _he_pair("44017000", "44016385", EHA, "True"),
        _he_pair("44017000", "44016370", HAA, "False"),
    ]

    he_rows, duplicate_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert [(r["old-entity"], r["status"], r["entity"]) for r in he_rows] == [("44017000", "301", "44016370")]
    assert "only one of 2 complete matches inside its LPA boundary" in he_rows[0]["notes"]
    assert [(r["old-entity"], r["status"], r["entity"]) for r in duplicate_rows] == [("44016385", "301", "44016370")]
    assert ambiguous == []


def test_resolve_handles_pairs_reported_with_he_as_entity_b():
    rows = [
        _check_row("44016385", "44017000", EHA, HE, a_out_of_bounds="True"),
        _check_row("44016370", "44017000", HAA, HE, a_out_of_bounds="False"),
    ]

    he_rows, duplicate_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert [(r["old-entity"], r["entity"]) for r in he_rows] == [("44017000", "44016370")]
    assert [(r["old-entity"], r["entity"]) for r in duplicate_rows] == [("44016385", "44016370")]
    assert ambiguous == []


def test_resolve_writes_nothing_for_an_ambiguous_entity_but_resolves_the_others():
    rows = [
        _he_pair("44017000", "44016385", EHA, "True"),
        _he_pair("44017000", "44016370", HAA, "False"),
        _he_pair("44017008", "44016382", HAA, "False"),
        _he_pair("44017008", "44016398", EHA, "False"),
        _he_pair("44017008", "44016438", EHA, "False"),
    ]

    he_rows, duplicate_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert [r["old-entity"] for r in he_rows] == ["44017000"]
    assert [r["old-entity"] for r in duplicate_rows] == ["44016385"]
    assert [a["old-entity"] for a in ambiguous] == ["44017008"]
    assert ambiguous[0]["matches"] == [
        ("44016382", HAA, False),
        ("44016398", EHA, False),
        ("44016438", EHA, False),
    ]


def test_resolve_never_redirects_duplicates_of_an_ambiguous_entity():
    rows = [_he_pair("1", "10", HAA, "False"), _he_pair("1", "20", EHA, "False")]

    he_rows, duplicate_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert he_rows == []
    assert duplicate_rows == []
    assert len(ambiguous) == 1


def test_resolve_single_match_is_a_plain_redirect_with_no_duplicates():
    rows = [_he_pair("5", "50", HAA, "")]

    he_rows, duplicate_rows, ambiguous = dedupe.resolve_complete_matches(rows)

    assert he_rows[0]["entity"] == "50"
    assert he_rows[0]["notes"] == "Redirect old entity complete match to LPA entity: local-authority:HAA"
    assert duplicate_rows == []
    assert ambiguous == []


def test_resolve_redirects_a_shared_duplicate_only_once():
    rows = [
        _he_pair("1", "44016376", HAA, "False"),
        _he_pair("1", "44016391", EHA, "True"),
        _he_pair("2", "44016375", HAA, "False"),
        _he_pair("2", "44016391", EHA, "True"),
    ]

    _, duplicate_rows, _ = dedupe.resolve_complete_matches(rows)

    assert [(r["old-entity"], r["entity"]) for r in duplicate_rows] == [("44016391", "44016376")]


def test_resolve_never_ends_an_entity():
    rows = [
        _he_pair("1", "10", HAA, "False"),
        _he_pair("1", "20", EHA, "True"),
        _he_pair("2", "30", HAA, "False"),
        _he_pair("3", "40", HAA, "False"),
        _he_pair("3", "50", EHA, "False"),
    ]

    he_rows, duplicate_rows, _ = dedupe.resolve_complete_matches(rows)

    assert {r["status"] for r in he_rows + duplicate_rows} == {"301"}
    assert all(r["entity"] for r in he_rows + duplicate_rows)


def test_extract_complete_matches_returns_only_resolved_historic_england_rows():
    rows = [
        _he_pair("1", "10", HAA, "False"),
        _he_pair("1", "20", EHA, "True"),
        _he_pair("2", "30", HAA, "False"),
        _he_pair("2", "40", EHA, "False"),
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


# ---------------------------------------------------------------- duplicate filter

def test_filter_out_of_bounds_duplicates_keeps_duplicate_that_is_a_redirect_target():
    old_entity = [{"old-entity": "44009285", "status": "301", "entity": "44016391"}]
    duplicates = [_new_match("44016391", entity="44016376")]

    assert dedupe.filter_out_of_bounds_duplicates(old_entity, duplicates, []) == duplicates


def test_filter_out_of_bounds_duplicates_skips_existing_source_and_new_source():
    old_entity = [{"old-entity": "44016391", "status": "301", "entity": "99"}]
    duplicates = [_new_match("44016391", entity="44016376"), _new_match("44016392", entity="44016377")]

    result = dedupe.filter_out_of_bounds_duplicates(old_entity, duplicates, [_new_match("44016392", entity="1")])

    assert result == []


def test_filter_out_of_bounds_duplicates_skips_loops():
    old_entity = [{"old-entity": "44016376", "status": "301", "entity": "44016391"}]
    duplicates = [_new_match("44016391", entity="44016376")]

    assert dedupe.filter_out_of_bounds_duplicates(old_entity, duplicates, []) == []


# ---------------------------------------------------------------- reporting

AMBIGUOUS = [{
    "old-entity": "44017008",
    "matches": [("44016382", HAA, False), ("44016398", EHA, False), ("44016438", EHA, False)],
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
    assert "44017008" in text and "44016398" in text and "inside its LPA boundary" in text


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
    checks = [_he_pair("44017000", "44016385", EHA, "True"), _he_pair("44017000", "44016370", HAA, "False")]

    code, rows = _run_main(monkeypatch, tmp_path, checks)

    assert code == 0
    assert sorted((r["old-entity"], r["entity"]) for r in rows) == [
        ("44016385", "44016370"),
        ("44017000", "44016370"),
    ]


def test_main_writes_the_safe_rows_then_fails_for_an_ambiguous_entity(monkeypatch, tmp_path):
    checks = [
        _he_pair("44017000", "44016385", EHA, "True"),
        _he_pair("44017000", "44016370", HAA, "False"),
        _he_pair("44017008", "44016382", HAA, "False"),
        _he_pair("44017008", "44016398", EHA, "False"),
        _he_pair("44017008", "44016438", EHA, "False"),
    ]

    code, rows = _run_main(monkeypatch, tmp_path, checks)

    assert code == 1
    old_entities = [r["old-entity"] for r in rows]
    assert sorted(old_entities) == ["44016385", "44017000"]
    assert "44017008" not in old_entities
    assert len(old_entities) == len(set(old_entities))


def test_main_is_a_no_op_on_a_second_run(monkeypatch, tmp_path):
    checks = [_he_pair("44017000", "44016385", EHA, "True"), _he_pair("44017000", "44016370", HAA, "False")]
    _, first = _run_main(monkeypatch, tmp_path, checks)

    _, second = _run_main(monkeypatch, tmp_path, checks, existing_rows=first)

    assert second == first


def test_main_leaves_a_manually_resolved_entity_alone_and_stops_failing(monkeypatch, tmp_path):
    checks = [
        _he_pair("44017008", "44016382", HAA, "False"),
        _he_pair("44017008", "44016398", EHA, "False"),
    ]
    manual = [_new_match("44017008", entity="44016382", notes="Manual decision")]

    code, rows = _run_main(monkeypatch, tmp_path, checks, existing_rows=manual)

    # the entity is already a source, so it is skipped by the conflict filter, but it is still
    # reported until the report stops listing it (its Historic England entity is retired)
    assert [r["old-entity"] for r in rows] == ["44017008"]
    assert rows[0]["notes"] == "Manual decision"
    assert code == 1
