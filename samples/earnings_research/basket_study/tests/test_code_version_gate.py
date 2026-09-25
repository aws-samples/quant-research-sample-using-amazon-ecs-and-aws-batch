"""Spec §5 code-version gate: the plan side must stamp the CURRENT git SHA,
the child side must read the SHA baked into its image, and the two must be
different sources — otherwise the gate compares a value to itself and a stale
image sails through.

No real git is invoked here: _git_code_version's subprocess calls are
monkeypatched.
"""
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import evaluate


class _Run:
    """Scripted subprocess.run replacement: returns queued results by argv[1]."""

    def __init__(self, sha="abc1234", dirty=False, fail=False):
        self.sha, self.dirty, self.fail = sha, dirty, fail
        self.calls = []

    def __call__(self, cmd, **kw):
        self.calls.append(list(cmd))
        if cmd[1] == "rev-parse":
            if self.fail:
                raise subprocess.CalledProcessError(128, cmd, stderr="not a repo")
            return subprocess.CompletedProcess(cmd, 0, stdout=self.sha + "\n", stderr="")
        if cmd[1] == "diff":
            return subprocess.CompletedProcess(cmd, 1 if self.dirty else 0)
        raise AssertionError(f"unexpected git call: {cmd}")


class TestGitCodeVersion:
    def test_returns_current_git_sha_cut_to_12(self, monkeypatch):
        # same format the image build bakes: first 12 characters of the full SHA
        run = _Run(sha="deadbeefcafe" + "0" * 28)
        monkeypatch.setattr(subprocess, "run", run)
        assert evaluate._git_code_version() == "deadbeefcafe"
        assert ["git", "rev-parse", "HEAD"] in run.calls

    def test_dirty_tree_aborts(self, monkeypatch):
        monkeypatch.setattr(subprocess, "run", _Run(dirty=True))
        with pytest.raises(SystemExit, match="uncommitted changes"):
            evaluate._git_code_version()

    def test_dirty_check_is_scoped_to_the_study_dir_and_vs_head(self, monkeypatch):
        run = _Run()
        monkeypatch.setattr(subprocess, "run", run)
        evaluate._git_code_version()
        diff = [c for c in run.calls if c[1] == "diff"][0]
        assert str(Path(evaluate.__file__).resolve().parent) in diff
        # vs HEAD, so staged-but-uncommitted changes are caught too
        assert "HEAD" in diff

    def test_git_failure_aborts_rather_than_falling_back(self, monkeypatch):
        monkeypatch.setattr(subprocess, "run", _Run(fail=True))
        with pytest.raises(SystemExit, match="cannot determine git SHA"):
            evaluate._git_code_version()

    def test_does_not_read_the_baked_code_version_file(self, monkeypatch):
        """The whole point: the plan side must NOT consult CODE_VERSION, or
        the child's assertion becomes a tautology."""
        monkeypatch.setattr(subprocess, "run", _Run(sha="fromgit"))
        monkeypatch.setattr(evaluate, "_code_version",
                            lambda: pytest.fail("_code_version must not be "
                                                "used on the plan side"))
        assert evaluate._git_code_version() == "fromgit"


class TestChildCodeVersion:
    def test_child_reads_baked_file(self, monkeypatch, tmp_path):
        """The child side reads CODE_VERSION only — never git."""
        monkeypatch.setattr(subprocess, "run",
                            lambda *a, **k: pytest.fail("child must not run git"))
        v = evaluate._code_version()
        baked = Path(evaluate.__file__).resolve().parent / "CODE_VERSION"
        assert v == (baked.read_text().strip() if baked.exists() else "dev")

    def test_plan_and_child_sources_diverge_on_a_stale_image(self, monkeypatch):
        """Simulates the real failure: image built at an old SHA, tree moved
        on. The two functions must return different values so the child's
        mismatch check fires."""
        monkeypatch.setattr(subprocess, "run", _Run(sha="newsha"))
        monkeypatch.setattr(evaluate, "_code_version", lambda: "oldsha")
        assert evaluate._git_code_version() != evaluate._code_version()
