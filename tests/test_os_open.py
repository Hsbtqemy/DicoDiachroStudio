from __future__ import annotations

from pathlib import Path

from dicodiachro_studio.services import os_open


class _Completed:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode


def test_reveal_in_file_manager_macos(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "sample.txt"
    target.write_text("ok", encoding="utf-8")

    calls: list[list[str]] = []

    def fake_run(cmd, check, capture_output, text):
        calls.append(cmd)
        return _Completed(0)

    monkeypatch.setattr(os_open.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(os_open.subprocess, "run", fake_run)

    assert os_open.reveal_in_file_manager(target) is True
    assert calls[0][0:2] == ["open", "-R"]
    assert calls[0][2] == str(target.resolve())


def test_reveal_in_file_manager_windows(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "sample.txt"
    target.write_text("ok", encoding="utf-8")

    calls: list[list[str]] = []

    def fake_run(cmd, check, capture_output, text):
        calls.append(cmd)
        return _Completed(0)

    monkeypatch.setattr(os_open.platform, "system", lambda: "Windows")
    monkeypatch.setattr(os_open.subprocess, "run", fake_run)

    assert os_open.reveal_in_file_manager(target) is True
    assert calls[0][0] == "explorer"
    assert calls[0][1].startswith("/select,")


def test_reveal_in_file_manager_linux_uses_directory(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "sample.txt"
    target.write_text("ok", encoding="utf-8")

    calls: list[list[str]] = []

    def fake_run(cmd, check, capture_output, text):
        calls.append(cmd)
        return _Completed(0)

    monkeypatch.setattr(os_open.platform, "system", lambda: "Linux")
    monkeypatch.setattr(os_open.subprocess, "run", fake_run)

    assert os_open.reveal_in_file_manager(target) is True
    assert calls[0] == ["xdg-open", str(tmp_path.resolve())]


def test_reveal_in_file_manager_handles_oserror(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "sample.txt"
    target.write_text("ok", encoding="utf-8")

    def fake_run(cmd, check, capture_output, text):
        raise OSError("boom")

    monkeypatch.setattr(os_open.platform, "system", lambda: "Linux")
    monkeypatch.setattr(os_open.subprocess, "run", fake_run)

    assert os_open.reveal_in_file_manager(target) is False
