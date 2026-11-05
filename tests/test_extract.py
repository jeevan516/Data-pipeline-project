"""Tests for re-run behaviour of the download step (no network needed)."""

from green_taxi import extract as extract_mod
from green_taxi.config import file_url


class FakeResponse:
    def __init__(self, body: bytes):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self.body


def test_builds_monthly_url():
    assert file_url("2025-02") == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-02.parquet"
    )


def test_skips_already_staged_file(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        extract_mod.requests, "get", lambda url, **kw: calls.append(url) or FakeResponse(b"new")
    )

    first = extract_mod.download_month("2025-01", tmp_path)
    second = extract_mod.download_month("2025-01", tmp_path)

    assert first == second and first.read_bytes() == b"new"
    assert len(calls) == 1  # second run reused the staged file
    assert not list(tmp_path.glob("*.part"))


def test_force_downloads_again(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_mod.requests, "get", lambda url, **kw: FakeResponse(b"v2"))
    (tmp_path / "green_tripdata_2025-01.parquet").write_bytes(b"v1")

    path = extract_mod.download_month("2025-01", tmp_path, force=True)

    assert path.read_bytes() == b"v2"
