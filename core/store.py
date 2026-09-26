"""
SQLite persistence layer.

Single small class, synchronous (SQLite calls are sub-millisecond), guarded by
a re-entrant lock so it is safe to call from the asyncio loop and threads.

Tables
------
users      one row per Telegram user (plan, credits, daily free usage, stats)
payments   every purchase attempt (razorpay / upi / manual) with status
jobs       translation job history
workers    translation worker URLs (managed from the admin panel)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional

log = logging.getLogger("store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id              INTEGER PRIMARY KEY,
    username        TEXT,
    first_name      TEXT,
    lang            TEXT    NOT NULL DEFAULT 'hi',
    credits         INTEGER NOT NULL DEFAULT 0,
    unlimited_until TEXT,
    banned          INTEGER NOT NULL DEFAULT 0,
    free_date       TEXT    NOT NULL DEFAULT '',
    free_used       INTEGER NOT NULL DEFAULT 0,
    total_files     INTEGER NOT NULL DEFAULT 0,
    total_chars     INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL,
    last_seen       TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS payments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    kind        TEXT    NOT NULL,          -- unlimited | credits
    millions    INTEGER NOT NULL DEFAULT 0,
    amount_inr  INTEGER NOT NULL,
    method      TEXT    NOT NULL,          -- razorpay | upi | manual
    status      TEXT    NOT NULL,          -- created | pending | paid | rejected | expired
    ref         TEXT,
    note        TEXT,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id, status);
CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    file_name   TEXT,
    lang        TEXT,
    chars       INTEGER NOT NULL DEFAULT 0,
    status      TEXT    NOT NULL,          -- queued | running | done | failed | cancelled
    error       TEXT,
    created_at  TEXT    NOT NULL,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_id, id);
CREATE TABLE IF NOT EXISTS workers (
    url         TEXT PRIMARY KEY,
    added_at    TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _today() -> str:
    return date.today().isoformat()


class Store:
    def __init__(self, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self._db.executescript(_SCHEMA)

    # ------------------------------------------------------------------ util
    def _q(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._db.execute(sql, tuple(params))

    def _one(self, sql: str, params: Iterable[Any] = ()) -> Optional[dict]:
        row = self._q(sql, params).fetchone()
        return dict(row) if row else None

    def _all(self, sql: str, params: Iterable[Any] = ()) -> List[dict]:
        return [dict(r) for r in self._q(sql, params).fetchall()]

    def close(self) -> None:
        with self._lock:
            try:
                self._db.close()
            except sqlite3.Error:
                pass

    # ----------------------------------------------------------------- users
    def touch_user(self, uid: int, username: Optional[str] = None, first_name: Optional[str] = None) -> dict:
        now = _now()
        with self._lock:
            self._q(
                "INSERT INTO users(id, username, first_name, created_at, last_seen) VALUES(?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET username=COALESCE(excluded.username, users.username), "
                "first_name=COALESCE(excluded.first_name, users.first_name), last_seen=excluded.last_seen",
                (uid, username, first_name, now, now),
            )
            return self.get_user(uid)  # type: ignore[return-value]

    def get_user(self, uid: int) -> Optional[dict]:
        return self._one("SELECT * FROM users WHERE id=?", (uid,))

    def set_lang(self, uid: int, lang: str) -> None:
        self.touch_user(uid)
        self._q("UPDATE users SET lang=? WHERE id=?", (lang, uid))

    def set_banned(self, uid: int, banned: bool) -> None:
        self.touch_user(uid)
        self._q("UPDATE users SET banned=? WHERE id=?", (1 if banned else 0, uid))

    def all_user_ids(self) -> List[int]:
        return [r["id"] for r in self._all("SELECT id FROM users WHERE banned=0 ORDER BY id")]

    def user_count(self) -> dict:
        week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        row = self._one(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN last_seen >= ? THEN 1 ELSE 0 END) AS active7d, "
            "SUM(CASE WHEN unlimited_until >= ? THEN 1 ELSE 0 END) AS unlimited, "
            "COALESCE(SUM(credits),0) AS credits_outstanding FROM users",
            (week_ago, _today()),
        ) or {}
        return {k: int(row.get(k) or 0) for k in ("total", "active7d", "unlimited", "credits_outstanding")}

    # ----------------------------------------------------------------- plans
    def is_unlimited(self, uid: int) -> bool:
        u = self.get_user(uid)
        return bool(u and u["unlimited_until"] and u["unlimited_until"] >= _today())

    def grant_unlimited(self, uid: int, days: int) -> str:
        """Extend (or start) the unlimited plan; returns new expiry date (ISO)."""
        self.touch_user(uid)
        u = self.get_user(uid) or {}
        cur = u.get("unlimited_until")
        base = date.fromisoformat(cur) if cur and cur >= _today() else date.today()
        until = (base + timedelta(days=max(1, days))).isoformat()
        self._q("UPDATE users SET unlimited_until=? WHERE id=?", (until, uid))
        return until

    def revoke_unlimited(self, uid: int) -> None:
        self._q("UPDATE users SET unlimited_until=NULL WHERE id=?", (uid,))

    def add_credits(self, uid: int, chars: int) -> int:
        """Add (or deduct, if negative) character credits. Returns new balance (never < 0)."""
        self.touch_user(uid)
        with self._lock:
            self._q("UPDATE users SET credits=MAX(0, credits + ?) WHERE id=?", (int(chars), uid))
            return int((self.get_user(uid) or {}).get("credits", 0))

    def _free_left(self, u: dict, free_daily: int) -> int:
        used = u["free_used"] if u["free_date"] == _today() else 0
        return max(0, free_daily - used)

    def available_chars(self, uid: int, free_daily: int) -> dict:
        """Snapshot of what the user can spend right now."""
        u = self.touch_user(uid)
        unlimited = bool(u["unlimited_until"] and u["unlimited_until"] >= _today())
        free_left = self._free_left(u, free_daily)
        credits = int(u["credits"])
        return {
            "unlimited": unlimited,
            "unlimited_until": u["unlimited_until"] if unlimited else None,
            "free_left": free_left,
            "free_daily": free_daily,
            "credits": credits,
            "total": free_left + credits,
        }

    def consume(self, uid: int, chars: int, free_daily: int) -> dict:
        """
        Deduct `chars` — free quota first, then credits.  Unlimited users pay nothing.
        Returns a breakdown that can be handed to refund().
        Raises ValueError when the balance is insufficient.
        """
        chars = max(0, int(chars))
        with self._lock:
            u = self.touch_user(uid)
            today = _today()
            if u["unlimited_until"] and u["unlimited_until"] >= today:
                self._q("UPDATE users SET total_files=total_files+1, total_chars=total_chars+? WHERE id=?",
                        (chars, uid))
                return {"free": 0, "credits": 0, "unlimited": True}
            used = u["free_used"] if u["free_date"] == today else 0
            free_left = max(0, free_daily - used)
            from_free = min(free_left, chars)
            from_credits = chars - from_free
            if from_credits > int(u["credits"]):
                raise ValueError("insufficient balance")
            self._q(
                "UPDATE users SET free_date=?, free_used=?, credits=credits-?, "
                "total_files=total_files+1, total_chars=total_chars+? WHERE id=?",
                (today, used + from_free, from_credits, chars, uid),
            )
            return {"free": from_free, "credits": from_credits, "unlimited": False}

    def refund(self, uid: int, breakdown: Optional[dict]) -> None:
        """Give back what consume() took (free quota only if still the same day)."""
        if not breakdown or breakdown.get("unlimited"):
            return
        with self._lock:
            u = self.get_user(uid)
            if not u:
                return
            free_back = int(breakdown.get("free", 0))
            if free_back and u["free_date"] == _today():
                self._q("UPDATE users SET free_used=MAX(0, free_used-?) WHERE id=?", (free_back, uid))
            cred_back = int(breakdown.get("credits", 0))
            if cred_back:
                self._q("UPDATE users SET credits=credits+? WHERE id=?", (cred_back, uid))

    # -------------------------------------------------------------- payments
    def create_payment(self, uid: int, kind: str, millions: int, amount_inr: int, method: str) -> int:
        now = _now()
        cur = self._q(
            "INSERT INTO payments(user_id, kind, millions, amount_inr, method, status, created_at, updated_at) "
            "VALUES(?,?,?,?,?,'created',?,?)",
            (uid, kind, int(millions), int(amount_inr), method, now, now),
        )
        return int(cur.lastrowid or 0)

    def get_payment(self, pid: int) -> Optional[dict]:
        return self._one("SELECT * FROM payments WHERE id=?", (pid,))

    def set_payment_status(self, pid: int, status: str, ref: Optional[str] = None, note: Optional[str] = None) -> None:
        self._q(
            "UPDATE payments SET status=?, ref=COALESCE(?, ref), note=COALESCE(?, note), updated_at=? WHERE id=?",
            (status, ref, note, _now(), pid),
        )

    def pending_payments(self, uid: Optional[int] = None, method: Optional[str] = None) -> List[dict]:
        sql = "SELECT * FROM payments WHERE status IN ('created','pending')"
        params: List[Any] = []
        if uid is not None:
            sql += " AND user_id=?"
            params.append(uid)
        if method:
            sql += " AND method=?"
            params.append(method)
        return self._all(sql + " ORDER BY id", params)

    def expire_old_pending(self, hours: int = 24) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        cur = self._q(
            "UPDATE payments SET status='expired', updated_at=? WHERE status IN ('created','pending') "
            "AND created_at < ? AND (ref IS NULL OR method='razorpay')",
            (_now(), cutoff),
        )
        return cur.rowcount or 0

    def revenue(self) -> dict:
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        row = self._one(
            "SELECT COALESCE(SUM(amount_inr),0) AS total_inr, COUNT(*) AS count, "
            "COALESCE(SUM(CASE WHEN substr(updated_at,1,7)=? THEN amount_inr ELSE 0 END),0) AS month_inr "
            "FROM payments WHERE status='paid'",
            (month,),
        ) or {}
        return {k: int(row.get(k) or 0) for k in ("total_inr", "count", "month_inr")}

    # ------------------------------------------------------------------ jobs
    def create_job(self, uid: int, file_name: str, lang: str, chars: int) -> int:
        cur = self._q(
            "INSERT INTO jobs(user_id, file_name, lang, chars, status, created_at) VALUES(?,?,?,?,'queued',?)",
            (uid, file_name, lang, int(chars), _now()),
        )
        return int(cur.lastrowid or 0)

    def update_job(self, jid: int, status: str, error: Optional[str] = None) -> None:
        finished = _now() if status in ("done", "failed", "cancelled") else None
        self._q("UPDATE jobs SET status=?, error=?, finished_at=COALESCE(?, finished_at) WHERE id=?",
                (status, (error or None) and str(error)[:500], finished, jid))

    def reset_stale_jobs(self) -> int:
        """After a crash, mark queued/running jobs as failed so history stays honest."""
        cur = self._q("UPDATE jobs SET status='failed', error='bot restarted', finished_at=? "
                      "WHERE status IN ('queued','running')", (_now(),))
        return cur.rowcount or 0

    def recent_jobs(self, uid: int, limit: int = 10) -> List[dict]:
        return self._all("SELECT * FROM jobs WHERE user_id=? ORDER BY id DESC LIMIT ?", (uid, limit))

    def job_stats(self) -> dict:
        today = _today()
        row = self._one(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done, "
            "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed, "
            "COALESCE(SUM(CASE WHEN status='done' THEN chars ELSE 0 END),0) AS chars, "
            "SUM(CASE WHEN substr(created_at,1,10)=? THEN 1 ELSE 0 END) AS today, "
            "COALESCE(SUM(CASE WHEN substr(created_at,1,10)=? AND status='done' THEN chars ELSE 0 END),0) AS today_chars "
            "FROM jobs",
            (today, today),
        ) or {}
        return {k: int(row.get(k) or 0) for k in ("total", "done", "failed", "chars", "today", "today_chars")}

    # --------------------------------------------------------------- workers
    def workers(self) -> List[str]:
        return [r["url"] for r in self._all("SELECT url FROM workers ORDER BY added_at, url")]

    def add_worker(self, url: str) -> None:
        self._q("INSERT OR IGNORE INTO workers(url, added_at) VALUES(?,?)", (url, _now()))

    def remove_worker(self, url: str) -> None:
        self._q("DELETE FROM workers WHERE url=?", (url,))

    # ------------------------------------------------------------- migration
    def migrate_legacy_json(self, path: str) -> None:
        """Import workers/users from the old data.json (v2) once, then rename it."""
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for w in data.get("workers", []):
                if isinstance(w, str) and w.startswith("http"):
                    self.add_worker(w.rstrip("/"))
            for k, v in (data.get("users_db") or {}).items():
                if not str(k).lstrip("-").isdigit() or not isinstance(v, dict):
                    continue
                uid = int(k)
                self.touch_user(uid)
                if v.get("lang"):
                    self.set_lang(uid, str(v["lang"]))
                until = v.get("premium_until")
                if until and str(until) >= _today():
                    self._q("UPDATE users SET unlimited_until=? WHERE id=?", (str(until), uid))
            os.replace(path, path + ".migrated")
            log.info("Migrated legacy %s", path)
        except Exception as exc:  # pragma: no cover
            log.warning("Legacy migration skipped: %s", exc)
