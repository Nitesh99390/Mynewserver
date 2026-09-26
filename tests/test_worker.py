"""Worker (app.py) unit tests – upstream Google call is mocked."""
import pytest
from fastapi.testclient import TestClient

import app as worker


@pytest.fixture
def client(monkeypatch):
    async def fake_google(texts, target, source):
        if any("FAIL" in t for t in texts):
            return None                                   # force split / failure path
        return [f"<{target}>{t}" for t in texts]

    monkeypatch.setattr(worker, "_google_request", fake_google)
    monkeypatch.setattr(worker, "WORKER_SECRET", "")
    return TestClient(worker.app)


def test_root_and_health(client):
    assert client.get("/").json()["status"] == "ok"
    h = client.get("/health").json()
    assert h["version"] == worker.VERSION and "load" in h and h["secured"] is False
    assert client.head("/").status_code == 200


def test_translate_ok(client):
    r = client.post("/translate", json={"text_list": ["Hello", "", "World"], "lang": "hi"})
    body = r.json()
    assert r.status_code == 200 and body["success"]
    assert body["translated"] == ["<hi>Hello", "", "<hi>World"] and body["failed"] == 0


def test_translate_partial_failure(client):
    r = client.post("/translate", json={"text_list": ["ok one", "FAIL me", "ok two"], "lang": "ta"})
    body = r.json()
    assert body["success"] and body["failed"] == 1
    assert body["translated"] == ["<ta>ok one", "FAIL me", "<ta>ok two"]      # original kept on failure


def test_validation(client):
    assert client.post("/translate", json={"text_list": ["x"], "lang": "not a lang!"}).status_code == 422
    assert client.post("/translate", json={"text_list": ["x"] * 501, "lang": "hi"}).status_code == 413
    assert client.post("/translate", json={"text_list": [], "lang": "hi"}).json()["translated"] == []


def test_secret(monkeypatch):
    monkeypatch.setattr(worker, "WORKER_SECRET", "s3cret")
    c = TestClient(worker.app)
    assert c.post("/translate", json={"text_list": ["x"], "lang": "hi"}).status_code == 401
    assert c.get("/health").status_code == 200                               # health stays public


def test_split_long_text():
    text = ("Sentence one. " * 400).strip()
    parts = worker.split_long_text(text, 500)
    assert all(len(p) <= 500 for p in parts) and " ".join(parts) == text
    assert worker.split_long_text("x" * 1200, 500) == ["x" * 500, "x" * 500, "x" * 200]


def test_parse_google_shapes():
    assert worker._parse_google(["a", "b"], 2) == ["a", "b"]
    assert worker._parse_google([["a", "en"], ["b", "en"]], 2) == ["a", "b"]
    assert worker._parse_google(["a", "en"], 1) == ["a"]
    assert worker._parse_google("a", 1) == ["a"]
    assert worker._parse_google(["a"], 2) is None
    assert worker._parse_google({"x": 1}, 1) is None
