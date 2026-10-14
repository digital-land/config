"""Exercise branch updates with a local Git remote; no GitHub credentials needed."""

import subprocess

import pytest

from bin import add_data


@pytest.fixture
def branch_repo(tmp_path, monkeypatch):
    remote = tmp_path / "remote.git"
    repo = tmp_path / "checkout"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "clone", str(remote), str(repo)], check=True, capture_output=True)
    monkeypatch.chdir(repo)

    def git(*args):
        return subprocess.check_output(["git", *args], text=True).strip()

    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.test")
    git("checkout", "-b", "main")
    (repo / "lookup.csv").write_text("entity\n42246912\n")
    git("add", ".")
    git("commit", "-m", "Initial lookup")
    git("push", "origin", "main")
    git("checkout", "-b", "shared")
    (repo / "pending.csv").write_text("pending submission\n")
    git("add", ".")
    git("commit", "-m", "Pending work")
    git("push", "origin", "shared")
    git("checkout", "main")
    (repo / "lookup.csv").write_text("entity\n42246912\n42246913\n")
    git("add", ".")
    git("commit", "-m", "Allocate on main")
    git("push", "origin", "main")
    git("checkout", "shared")
    return repo, git


def test_update_branch_preserves_pending_work_and_main_allocations(branch_repo):
    repo, git = branch_repo
    add_data.sync_branch_with_main()
    assert "42246913" in (repo / "lookup.csv").read_text()
    assert (repo / "pending.csv").read_text() == "pending submission\n"
    assert git("merge-base", "HEAD", "origin/main") == git("rev-parse", "origin/main")


def test_conflict_stops_update_without_overwriting_branch(branch_repo):
    repo, git = branch_repo
    (repo / "lookup.csv").write_text("entity\n42246912\n999\n")
    git("add", ".")
    git("commit", "-m", "Conflicting allocation")
    head = git("rev-parse", "HEAD")
    with pytest.raises(SystemExit):
        add_data.sync_branch_with_main()
    assert git("rev-parse", "HEAD") == head
    assert (repo / "lookup.csv").read_text() == "entity\n42246912\n999\n"
