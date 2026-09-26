import os
import tempfile

import pytest

from bot import MILLION
from bot import credits_price, fmt_chars, fulfil, parse_millions, plan_summary
from bot import Store

FREE = MILLION


@pytest.fixture
def store():
    d = tempfile.mkdtemp()
    s = Store(os.path.join(d, "t.sqlite3"))
    yield s
    s.close()


def test_free_quota_then_credits(store):
    uid = 42
    store.touch_user(uid, "nitesh", "Nitesh")
    a = store.available_chars(uid, FREE)
    assert a["free_left"] == FREE and a["credits"] == 0 and not a["unlimited"]

    # 600k from free
    b = store.consume(uid, 600_000, FREE)
    assert b == {"free": 600_000, "credits": 0, "unlimited": False}
    assert store.available_chars(uid, FREE)["free_left"] == 400_000

    # 700k -> 400k free + 300k credits -> insufficient (0 credits)
    with pytest.raises(ValueError):
        store.consume(uid, 700_000, FREE)
    # nothing deducted on failure
    assert store.available_chars(uid, FREE)["free_left"] == 400_000

    store.add_credits(uid, 2 * MILLION)
    b = store.consume(uid, 700_000, FREE)
    assert b == {"free": 400_000, "credits": 300_000, "unlimited": False}
    a = store.available_chars(uid, FREE)
    assert a["free_left"] == 0 and a["credits"] == 2 * MILLION - 300_000

    # refund restores both
    store.refund(uid, b)
    a = store.available_chars(uid, FREE)
    assert a["free_left"] == 400_000 and a["credits"] == 2 * MILLION


def test_unlimited(store):
    uid = 7
    until = store.grant_unlimited(uid, 30)
    assert store.is_unlimited(uid)
    b = store.consume(uid, 50 * MILLION, FREE)
    assert b["unlimited"] is True
    a = store.available_chars(uid, FREE)
    assert a["unlimited"] and a["unlimited_until"] == until
    # extension stacks on current expiry
    until2 = store.grant_unlimited(uid, 30)
    assert until2 > until
    store.revoke_unlimited(uid)
    assert not store.is_unlimited(uid)


def test_payments_flow(store):
    uid = 9
    store.touch_user(uid)
    pid = store.create_payment(uid, "credits", 4, credits_price(4), "upi")
    store.set_payment_status(pid, "pending", ref="123456789012")
    assert [p["id"] for p in store.pending_payments(method="upi")] == [pid]
    pay = store.get_payment(pid)
    msg = fulfil(store, pay, note="test")
    assert "4M" in msg
    assert store.get_payment(pid)["status"] == "paid"
    assert store.available_chars(uid, FREE)["credits"] == 4 * MILLION
    assert store.revenue()["total_inr"] == 8

    pid2 = store.create_payment(uid, "unlimited", 0, 100, "razorpay")
    fulfil(store, store.get_payment(pid2))
    assert store.is_unlimited(uid)


def test_jobs_and_stats(store):
    uid = 3
    store.touch_user(uid)
    jid = store.create_job(uid, "a.epub", "hi", 1000)
    store.update_job(jid, "running")
    store.update_job(jid, "done")
    js = store.job_stats()
    assert js["total"] == 1 and js["done"] == 1 and js["chars"] == 1000
    assert store.recent_jobs(uid)[0]["status"] == "done"
    store.create_job(uid, "b.epub", "hi", 5)
    assert store.reset_stale_jobs() == 1
    assert store.get_user(uid) is not None


def test_workers_persist(store):
    store.add_worker("https://a.onrender.com")
    store.add_worker("https://a.onrender.com")
    store.add_worker("https://b.vercel.app")
    assert store.workers() == ["https://a.onrender.com", "https://b.vercel.app"]
    store.remove_worker("https://a.onrender.com")
    assert store.workers() == ["https://b.vercel.app"]


def test_legacy_migration(store, tmp_path):
    import json
    from datetime import date, timedelta

    p = tmp_path / "data.json"
    p.write_text(json.dumps({
        "workers": ["https://old.onrender.com/"],
        "users_db": {"11": {"lang": "ta", "premium_until": (date.today() + timedelta(days=5)).isoformat()},
                     "12": {"lang": "bn", "premium_until": "2000-01-01"}},
    }))
    store.migrate_legacy_json(str(p))
    assert store.workers() == ["https://old.onrender.com"]
    assert store.get_user(11)["lang"] == "ta" and store.is_unlimited(11)
    assert store.get_user(12)["lang"] == "bn" and not store.is_unlimited(12)
    assert not p.exists() and (tmp_path / "data.json.migrated").exists()


@pytest.mark.parametrize("text,expected", [
    ("4", 4), ("4M", 4), ("6 million", 6), ("2,000,000", 2), ("10m", 10), ("1", 1),
    ("0", None), ("abc", None), ("2.5", None), ("999999", None), ("", None), ("-3", None),
])
def test_parse_millions(text, expected):
    assert parse_millions(text) == expected


def test_pricing_and_format():
    assert credits_price(4) == 8
    assert plan_summary("unlimited", 0) == ("Unlimited plan • 30 days", 100)
    assert plan_summary("credits", 6) == ("6M characters credits", 12)
    assert fmt_chars(1_000_000) == "1M"
    assert fmt_chars(2_500_000) == "2.5M"
    assert fmt_chars(12_000) == "12K"
    assert fmt_chars(999) == "999"
