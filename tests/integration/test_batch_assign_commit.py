"""Tests for batch_assign_entities.commit_and_push against real git repositories (a local bare
repo stands in for origin), with only the gh calls mocked."""

import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / ".github/scripts"))

from batch_assign_entities import commit_and_push, ensure_pull_request


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def clone(origin, path):
    git(origin.parent, "clone", "-q", str(origin), str(path))
    git(path, "config", "user.name", "test")
    git(path, "config", "user.email", "test@example.com")
    return path


def append_lookup(repo, line):
    lookup = repo / "pipeline" / "conservation-area" / "lookup.csv"
    with open(lookup, "a") as f:
        f.write(f"{line}\n")


def file_on(origin, branch):
    return git(origin, "show", f"{branch}:pipeline/conservation-area/lookup.csv").splitlines()


@pytest.fixture
def origin(tmp_path):
    """A bare origin whose main has a lookup.csv, plus a working clone checked out on main."""
    origin = tmp_path / "origin.git"
    git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))

    seed = clone(origin, tmp_path / "seed")
    (seed / "pipeline" / "conservation-area").mkdir(parents=True)
    append_lookup(seed, "prefix,resource,entry-number,organisation,reference,entity")
    git(seed, "add", ".")
    git(seed, "commit", "-q", "-m", "seed")
    git(seed, "push", "-q", "origin", "HEAD:main")
    return origin


@pytest.fixture
def work(origin, tmp_path, monkeypatch):
    work = clone(origin, tmp_path / "work")
    monkeypatch.chdir(work)
    return work


def test_pushes_to_main_by_default_without_touching_prs(origin, work):
    append_lookup(work, "conservation-area,,,,CA1,44000001")

    with patch("batch_assign_entities.ensure_pull_request") as ensure_pr:
        commit_and_push(triggered_by="test", success_count=1, scope="odp")

    assert file_on(origin, "main")[-1] == "conservation-area,,,,CA1,44000001"
    ensure_pr.assert_not_called()


def test_creates_missing_branch_from_main_and_opens_pr(origin, work, tmp_path):
    # main moves on after the run checked out, as it would if another workflow pushed meanwhile
    other = clone(origin, tmp_path / "other")
    (other / "pipeline" / "tree").mkdir()
    (other / "pipeline" / "tree" / "lookup.csv").write_text("tree,,,,T1,1\n")
    git(other, "add", ".")
    git(other, "commit", "-q", "-m", "elsewhere")
    git(other, "push", "-q", "origin", "HEAD:main")

    append_lookup(work, "conservation-area,,,,CA1,44000001")
    git(work, "checkout", "-q", "--detach")  # HEAD needn't be a local branch named like the target

    with patch("batch_assign_entities.ensure_pull_request") as ensure_pr:
        commit_and_push(triggered_by="test", success_count=1, scope="odp", branch="config-manager-update")

    assert file_on(origin, "config-manager-update")[-1] == "conservation-area,,,,CA1,44000001"
    assert git(origin, "show", "config-manager-update:pipeline/tree/lookup.csv") == "tree,,,,T1,1"
    assert len(file_on(origin, "main")) == 1
    ensure_pr.assert_called_once()
    assert ensure_pr.call_args.args[0] == "config-manager-update"


def test_rebases_onto_existing_branch_keeping_commits_pushed_since_checkout(origin, work, tmp_path):
    git(work, "push", "-q", "origin", "HEAD:refs/heads/config-manager-update")
    git(work, "fetch", "-q", "origin", "config-manager-update")
    git(work, "checkout", "-q", "-B", "config-manager-update", "origin/config-manager-update")

    # Manage commits to the branch (in another collection) while the batch assign is running
    manage = clone(origin, tmp_path / "manage")
    git(manage, "checkout", "-q", "config-manager-update")
    (manage / "pipeline" / "tree").mkdir()
    (manage / "pipeline" / "tree" / "lookup.csv").write_text("tree,,,,T1,1\n")
    git(manage, "add", ".")
    git(manage, "commit", "-q", "-m", "manage")
    git(manage, "push", "-q", "origin", "config-manager-update")

    append_lookup(work, "conservation-area,,,,CA1,44000001")

    with patch("batch_assign_entities.ensure_pull_request"):
        commit_and_push(triggered_by="test", success_count=1, scope="odp", branch="config-manager-update")

    assert file_on(origin, "config-manager-update")[-1] == "conservation-area,,,,CA1,44000001"
    assert git(origin, "show", "config-manager-update:pipeline/tree/lookup.csv") == "tree,,,,T1,1"
    assert git(origin, "log", "-1", "--format=%s", "config-manager-update") == "odp - Batch assign entities update (1 successful resource(s))"


def test_skips_commit_when_nothing_changed(origin, work):
    before = git(origin, "rev-parse", "main")

    with patch("batch_assign_entities.ensure_pull_request") as ensure_pr:
        commit_and_push(triggered_by="test", success_count=1, scope="odp", branch="config-manager-update")

    assert git(origin, "rev-parse", "main") == before
    assert git(origin, "ls-remote", "--heads", ".", "config-manager-update") == ""
    ensure_pr.assert_not_called()


@patch("batch_assign_entities.run_command")
def test_ensure_pull_request_leaves_an_open_pr_alone(mock_run):
    mock_run.return_value = "123"

    ensure_pull_request("config-manager-update", "label")

    assert mock_run.call_count == 1
    assert mock_run.call_args.args[0][:3] == ["gh", "pr", "list"]


@patch("batch_assign_entities.run_command")
def test_ensure_pull_request_opens_one_into_main_when_none_is_open(mock_run):
    mock_run.return_value = ""

    ensure_pull_request("config-manager-update", "odp - Batch assign entities update (1 successful resource(s))")

    create = mock_run.call_args_list[1].args[0]
    assert create[:3] == ["gh", "pr", "create"]
    assert create[create.index("--base") + 1] == "main"
    assert create[create.index("--head") + 1] == "config-manager-update"


def test_batch_assign_workflow_shares_the_evening_pipelines_concurrency_group():
    """The evening merge deletes config-manager-update; sharing a group makes it wait for a
    running batch assign instead of pulling the branch out from under it."""
    import yaml

    workflows = Path(__file__).parent.parent.parent / ".github" / "workflows"
    batch_assign = yaml.safe_load((workflows / "batch-assign.yml").read_text())
    evening = yaml.safe_load((workflows / "config-evening-pipeline.yml").read_text())

    assert batch_assign["concurrency"]["group"] == evening["concurrency"]["group"]
    assert batch_assign["concurrency"]["cancel-in-progress"] is False


def test_batch_assign_workflow_keeps_the_artifact_names_manage_imports():
    """Manage's flagged-resources list finds these by name, across every workflow in the repo."""
    content = (Path(__file__).parent.parent.parent / ".github" / "workflows" / "batch-assign.yml").read_text()

    assert "runs-on: ubuntu-22.04" in content
    assert "sudo apt-get install gdal-bin -y" in content
    assert "name: batch-assign-${{ matrix.scope }}-output" in content
    assert "path: batch_assign_summary*.csv" in content
    assert "--branch \"$BRANCH\"" in content
