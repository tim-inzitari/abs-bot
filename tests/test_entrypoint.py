import pytest

from app import entrypoint


def test_entrypoint_runs_bot(monkeypatch) -> None:
    called = {"bot": 0, "updater": 0}
    monkeypatch.setenv("APP_ROLE", "bot")
    monkeypatch.setattr(entrypoint, "run_bot", lambda: called.__setitem__("bot", called["bot"] + 1))
    monkeypatch.setattr(entrypoint, "run_updater", lambda: called.__setitem__("updater", called["updater"] + 1))

    entrypoint.main()

    assert called == {"bot": 1, "updater": 0}


def test_entrypoint_runs_updater(monkeypatch) -> None:
    called = {"bot": 0, "updater": 0}
    monkeypatch.setenv("APP_ROLE", "updater")
    monkeypatch.setattr(entrypoint, "run_bot", lambda: called.__setitem__("bot", called["bot"] + 1))
    monkeypatch.setattr(entrypoint, "run_updater", lambda: called.__setitem__("updater", called["updater"] + 1))

    entrypoint.main()

    assert called == {"bot": 0, "updater": 1}


def test_entrypoint_rejects_unknown_role(monkeypatch) -> None:
    monkeypatch.setenv("APP_ROLE", "unknown")
    with pytest.raises(SystemExit, match="Unsupported APP_ROLE"):
        entrypoint.main()
