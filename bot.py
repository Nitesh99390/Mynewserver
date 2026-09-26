"""
EPUB Translator – Master Bot  v3  (single-file edition)
=======================================================
Runs on your own server (Oracle VPS).  Everything the master needs lives in
THIS ONE FILE: config, SQLite store, plans/payments, EPUB engine, worker pool,
job queue, Telegram UI and handlers.

The master NEVER calls Google (or any translation API) itself.  All text
batches are sent to your workers (app.py on Render / Vercel / anywhere) which
do the actual translation.  The master only manages workers: load-balancing,
failover, cooldown, keep-alive pings, health checks.

    pip install -r requirements-bot.txt
    cp .env.example .env        # fill API_ID, API_HASH, BOT_TOKEN, OWNER_ID, WORKER_SECRET, WORKERS
    python bot.py

See README.md for the full deployment guide (systemd unit included).
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import logging.handlers
import os
import posixpath
import re
import secrets
import shutil
import signal
import sqlite3
import tempfile
import threading
import time
import warnings
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote
from xml.etree import ElementTree as ET

import aiohttp
from bs4 import BeautifulSoup, Comment, NavigableString, XMLParsedAsHTMLWarning
from bs4.element import CData, Declaration, Doctype, ProcessingInstruction
from dotenv import load_dotenv
from pyrogram import Client, enums, filters
from pyrogram.errors import FloodWait, MessageNotModified, RPCError
from pyrogram.types import CallbackQuery, Message
from pyrogram.types import InlineKeyboardButton as B
from pyrogram.types import InlineKeyboardMarkup as KB
from pyrogram.types import KeyboardButton, ReplyKeyboardMarkup

__version__ = "3.1.0"

log = logging.getLogger("bot")

# =============================================================================
# Configuration
# =============================================================================
"""
Central configuration (environment / .env driven).

All tunables live here so bot.py and the core modules never read os.environ
directly.  Every value has a safe default so the bot boots with only the four
mandatory Telegram variables set.
"""

load_dotenv()

MILLION = 1_000_000


def _int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, default)).strip() or default)
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(str(os.environ.get(name, default)).strip() or default)
    except (TypeError, ValueError):
        return default


def _str(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default) or default).strip()


def _ids(name: str) -> List[int]:
    out: List[int] = []
    for part in _str(name).replace(";", ",").split(","):
        part = part.strip()
        if part.lstrip("-").isdigit():
            out.append(int(part))
    return out


@dataclass
class Settings:
    # --- Telegram -----------------------------------------------------------
    api_id: int = field(default_factory=lambda: _int("API_ID", 0))
    api_hash: str = field(default_factory=lambda: _str("API_HASH"))
    bot_token: str = field(default_factory=lambda: _str("BOT_TOKEN"))
    owner_id: int = field(default_factory=lambda: _int("OWNER_ID", 0))
    extra_admins: List[int] = field(default_factory=lambda: _ids("ADMIN_IDS"))

    # --- Workers ------------------------------------------------------------
    worker_secret: str = field(default_factory=lambda: _str("WORKER_SECRET"))
    seed_workers: List[str] = field(
        default_factory=lambda: [w.strip().rstrip("/") for w in _str("WORKERS").split(",") if w.strip()]
    )
    worker_timeout_s: float = field(default_factory=lambda: _float("WORKER_TIMEOUT", 90))
    worker_cooldown_s: float = field(default_factory=lambda: _float("WORKER_COOLDOWN", 90))
    keepalive_interval_s: float = field(default_factory=lambda: _float("KEEPALIVE_INTERVAL", 300))
    batch_items: int = field(default_factory=lambda: _int("BATCH_ITEMS", 40))
    batch_chars: int = field(default_factory=lambda: _int("BATCH_CHARS", 4000))
    parallel_batches: int = field(default_factory=lambda: _int("PARALLEL_BATCHES", 10))

    # --- Plans & pricing ----------------------------------------------------
    free_daily_chars: int = field(default_factory=lambda: _int("FREE_DAILY_CHARS", MILLION))
    unlimited_price_inr: int = field(default_factory=lambda: _int("UNLIMITED_PRICE_INR", 100))
    unlimited_days: int = field(default_factory=lambda: _int("UNLIMITED_DAYS", 30))
    credit_price_per_million_inr: int = field(default_factory=lambda: _int("CREDIT_PRICE_PER_MILLION_INR", 2))
    min_credit_millions: int = field(default_factory=lambda: _int("MIN_CREDIT_MILLIONS", 1))
    max_credit_millions: int = field(default_factory=lambda: _int("MAX_CREDIT_MILLIONS", 500))

    # --- Payments -----------------------------------------------------------
    razorpay_key_id: str = field(default_factory=lambda: _str("RAZORPAY_KEY_ID"))
    razorpay_key_secret: str = field(default_factory=lambda: _str("RAZORPAY_KEY_SECRET"))
    upi_id: str = field(default_factory=lambda: _str("UPI_ID"))
    upi_name: str = field(default_factory=lambda: _str("UPI_NAME", "EPUB Translator"))
    support_contact: str = field(default_factory=lambda: _str("SUPPORT_CONTACT"))

    # --- Runtime ------------------------------------------------------------
    data_dir: str = field(default_factory=lambda: _str("DATA_DIR", "data"))
    max_file_mb: int = field(default_factory=lambda: _int("MAX_FILE_MB", 50))
    max_concurrent_jobs: int = field(default_factory=lambda: _int("MAX_CONCURRENT_JOBS", 3))
    pending_ttl_s: int = field(default_factory=lambda: _int("PENDING_TTL", 900))
    log_level: str = field(default_factory=lambda: _str("LOG_LEVEL", "INFO").upper())

    # ----------------------------------------------------------------------
    @property
    def admins(self) -> Set[int]:
        ids = set(self.extra_admins)
        if self.owner_id:
            ids.add(self.owner_id)
        return ids

    def is_admin(self, uid: int) -> bool:
        return uid in self.admins

    @property
    def db_file(self) -> str:
        return os.path.join(self.data_dir, "bot.sqlite3")

    @property
    def legacy_json(self) -> str:
        return _str("DATA_FILE", "data.json")

    @property
    def download_dir(self) -> str:
        return os.path.join(self.data_dir, "downloads")

    @property
    def log_file(self) -> str:
        return os.path.join(self.data_dir, "bot.log")

    def validate(self) -> None:
        missing = [n for n, v in (("API_ID", self.api_id), ("API_HASH", self.api_hash),
                                  ("BOT_TOKEN", self.bot_token), ("OWNER_ID", self.owner_id)) if not v]
        if missing:
            raise SystemExit(f"Missing required environment variables: {', '.join(missing)} (see .env.example)")
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)


settings = Settings()

# --------------------------------------------------------------------------- #
# Languages (code -> display name).  Order = display order.
# --------------------------------------------------------------------------- #
LANGUAGES = {
    "hi": "Hindi", "bn": "Bengali", "ta": "Tamil", "te": "Telugu",
    "mr": "Marathi", "gu": "Gujarati", "kn": "Kannada", "ml": "Malayalam",
    "pa": "Punjabi", "ur": "Urdu", "or": "Odia", "as": "Assamese",
    "ne": "Nepali", "si": "Sinhala", "en": "English", "es": "Spanish",
    "fr": "French", "de": "German", "pt": "Portuguese", "it": "Italian",
    "ru": "Russian", "tr": "Turkish", "ar": "Arabic", "fa": "Persian",
    "id": "Indonesian", "ms": "Malay", "vi": "Vietnamese", "th": "Thai",
    "zh-CN": "Chinese", "ja": "Japanese", "ko": "Korean",
}


def lang_name(code: str) -> str:
    return LANGUAGES.get(code, code or "?")


# =============================================================================
# Storage (SQLite)
# =============================================================================
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


# =============================================================================
# Plans & payments
# =============================================================================
"""
Pricing + payment helpers.

Plans
-----
Free       FREE_DAILY_CHARS characters per day (default 1,000,000), resets daily.
Unlimited  ₹UNLIMITED_PRICE_INR for UNLIMITED_DAYS days (default ₹100 / 30 days).
Credits    N million characters at ₹CREDIT_PRICE_PER_MILLION_INR each (default ₹2/M),
           any N between MIN_CREDIT_MILLIONS and MAX_CREDIT_MILLIONS.  Credits never expire.

Payment methods
---------------
Razorpay   payment links created via the REST API (aiohttp, non-blocking), verified
           on demand by polling the link status.
UPI        manual: user pays to UPI_ID, sends UTR, admin approves with one tap.
"""

# --------------------------------------------------------------------------- #
# Pricing
# --------------------------------------------------------------------------- #
def credits_price(millions: int) -> int:
    return int(millions) * settings.credit_price_per_million_inr


def parse_millions(text: str) -> Optional[int]:
    """'4', '4M', '4 million', '4,000,000' -> 4.  None when invalid / out of range."""
    s = (text or "").strip().lower().replace(",", "").replace("₹", "")
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(m|mn|mil|million|lakh|l|k)?\b", s)
    if not m:
        return None
    num = float(m.group(1))
    unit = m.group(2)
    if unit in ("k",):
        num = num * 1_000 / MILLION
    elif unit in ("lakh", "l"):
        num = num * 100_000 / MILLION
    elif not unit and num >= 100_000:          # raw character count
        num = num / MILLION
    if num != int(num) or int(num) < settings.min_credit_millions or int(num) > settings.max_credit_millions:
        return None
    return int(num)


def fmt_chars(n: int) -> str:
    n = int(n or 0)
    if n >= MILLION:
        v = n / MILLION
        return f"{v:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"{n / 1_000:.1f}K".replace(".0K", "K")
    return str(n)


def plan_summary(kind: str, millions: int) -> Tuple[str, int]:
    """Human description + price for a plan."""
    if kind == "unlimited":
        return f"Unlimited plan • {settings.unlimited_days} days", settings.unlimited_price_inr
    return f"{millions}M characters credits", credits_price(millions)


def fulfil(store: Store, pay: dict, note: str = "") -> str:
    """Mark a payment as paid and activate the plan. Returns a message for the user."""
    store.set_payment_status(pay["id"], "paid", note=note or None)
    if pay["kind"] == "unlimited":
        until = store.grant_unlimited(pay["user_id"], settings.unlimited_days)
        return f"⭐ Unlimited plan active till {until}"
    chars = int(pay["millions"]) * MILLION
    bal = store.add_credits(pay["user_id"], chars)
    return f"💎 {fmt_chars(chars)} characters added • Balance {fmt_chars(bal)}"


def upi_link(amount_inr: int, note: str) -> str:
    return (f"upi://pay?pa={quote(settings.upi_id)}&pn={quote(settings.upi_name)}"
            f"&am={amount_inr}&cu=INR&tn={quote(note)}")


# --------------------------------------------------------------------------- #
# Razorpay (REST via aiohttp – no blocking SDK calls inside the event loop)
# --------------------------------------------------------------------------- #
class Razorpay:
    BASE = "https://api.razorpay.com/v1"

    def __init__(self) -> None:
        self.key = settings.razorpay_key_id
        self.secret = settings.razorpay_key_secret

    @property
    def enabled(self) -> bool:
        return bool(self.key and self.secret)

    def _auth(self) -> aiohttp.BasicAuth:
        return aiohttp.BasicAuth(self.key, self.secret)

    async def create_link(self, uid: int, kind: str, millions: int, amount_inr: int, pid: int) -> Tuple[str, str]:
        """Return (link_id, short_url)."""
        desc, _ = plan_summary(kind, millions)
        body = {
            "amount": int(amount_inr) * 100,
            "currency": "INR",
            "accept_partial": False,
            "description": f"EPUB Translator – {desc}",
            "reference_id": f"epub-{pid}",
            "notify": {"sms": False, "email": False},
            "reminder_enable": False,
            "notes": {"user_id": str(uid), "payment_id": str(pid), "kind": kind, "millions": str(millions)},
        }
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as s:
            async with s.post(f"{self.BASE}/payment_links", json=body, auth=self._auth()) as r:
                data = await r.json(content_type=None)
                if r.status >= 300:
                    raise RuntimeError(str(data.get("error", {}).get("description", r.status))[:120])
                return data["id"], data["short_url"]

    async def is_paid(self, link_id: str) -> Optional[bool]:
        """True = paid, False = not yet, None = could not check."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
                async with s.get(f"{self.BASE}/payment_links/{link_id}", auth=self._auth()) as r:
                    if r.status != 200:
                        return None
                    data = await r.json(content_type=None)
                    return data.get("status") == "paid"
        except (aiohttp.ClientError, ValueError, TimeoutError) as exc:
            log.warning("razorpay check failed: %s", exc)
            return None


# =============================================================================
# EPUB engine (parse / batch / rebuild)
# =============================================================================
"""
EPUB engine – analyse, translate and rebuild EPUB files *in place*.

Instead of re-generating the book with ebooklib (which can drop covers, CSS
or TOC entries), the original ZIP is copied entry-by-entry and only the
XHTML documents (and NCX labels) are replaced.  Everything else – images,
fonts, CSS, metadata, layout – survives byte-for-byte.

Public API
----------
analyse(path)  -> Analysis              (sync, run in a thread)
translate(analysis, lang, translate_batch, on_progress, cancel) -> out_path
"""

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

SKIP_TAGS = {"script", "style", "code", "pre", "kbd", "samp", "var", "math", "svg", "textarea", "head", "title"}
_NON_TEXT = re.compile(r"^[\W\d_]+$", re.UNICODE)          # digits / punctuation only
_IGNORED_NODE_TYPES = (Comment, CData, Declaration, Doctype, ProcessingInstruction)
_NS_CONTAINER = "{urn:oasis:names:tc:opendocument:xmlns:container}"
_NS_OPF = "{http://www.idpf.org/2007/opf}"
_XHTML_TYPES = {"application/xhtml+xml", "text/html", "application/x-dtbook+xml"}
_XHTML_EXT = (".xhtml", ".html", ".htm", ".xml")


class EpubError(Exception):
    """User-facing, safe to show in Telegram."""


@dataclass
class DocInfo:
    name: str                                  # zip entry name
    soup: BeautifulSoup
    nodes: List[NavigableString]
    is_ncx: bool = False


@dataclass
class Analysis:
    path: str
    title: str
    docs: List[DocInfo] = field(default_factory=list)
    opf_name: Optional[str] = None
    total_nodes: int = 0
    total_chars: int = 0
    language: str = ""

    def texts(self) -> List[str]:
        return [str(n) for d in self.docs for n in d.nodes]


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #
def _collect_nodes(soup: BeautifulSoup) -> List[NavigableString]:
    """All translatable text nodes in document order; structure untouched."""
    nodes: List[NavigableString] = []
    root = soup.body or soup                      # never touch <head> (title, meta, style)
    for node in root.descendants:
        if not isinstance(node, NavigableString) or isinstance(node, _IGNORED_NODE_TYPES):
            continue
        text = str(node).strip()
        if not text or _NON_TEXT.match(text):
            continue
        parent = node.parent
        skip = False
        while parent is not None and getattr(parent, "name", None):
            if parent.name.lower() in SKIP_TAGS:
                skip = True
                break
            parent = parent.parent
        if not skip:
            nodes.append(node)
    return nodes


def _ncx_nodes(soup: BeautifulSoup) -> List[NavigableString]:
    """Only navLabel/text and docTitle/text in an NCX."""
    out: List[NavigableString] = []
    for tag in soup.find_all("text"):
        for child in tag.children:
            if isinstance(child, NavigableString) and not isinstance(child, _IGNORED_NODE_TYPES) \
                    and str(child).strip() and not _NON_TEXT.match(str(child).strip()):
                out.append(child)
    return out


def _find_opf(zf: zipfile.ZipFile) -> Optional[str]:
    try:
        root = ET.fromstring(zf.read("META-INF/container.xml"))
        for rf in root.iter(f"{_NS_CONTAINER}rootfile"):
            full = rf.get("full-path")
            if full and full in zf.namelist():
                return full
    except (KeyError, ET.ParseError):
        pass
    for n in zf.namelist():                      # fallback: first *.opf
        if n.lower().endswith(".opf"):
            return n
    return None


def _manifest(zf: zipfile.ZipFile, opf_name: str) -> Tuple[List[str], Optional[str], str, str]:
    """Return (xhtml entry names in spine order, ncx entry name, title, language)."""
    base = posixpath.dirname(opf_name)
    names = set(zf.namelist())

    def resolve(href: str) -> str:
        href = href.split("#", 1)[0]
        try:
            from urllib.parse import unquote
            href = unquote(href)
        except Exception:  # pragma: no cover
            pass
        return posixpath.normpath(posixpath.join(base, href)) if base else posixpath.normpath(href)

    docs: List[str] = []
    ncx: Optional[str] = None
    title, lang = "", ""
    try:
        root = ET.fromstring(zf.read(opf_name))
    except ET.ParseError as exc:
        raise EpubError("EPUB ka package file (OPF) corrupt hai.") from exc

    items: Dict[str, Tuple[str, str]] = {}
    for it in root.iter(f"{_NS_OPF}item"):
        iid, href, mt = it.get("id"), it.get("href"), (it.get("media-type") or "").lower()
        if iid and href:
            items[iid] = (resolve(href), mt)
    spine_ids = [ref.get("idref") for ref in root.iter(f"{_NS_OPF}itemref") if ref.get("idref")]
    seen = set()
    for iid in spine_ids + list(items.keys()):        # spine order first, then the rest
        if iid in seen or iid not in items:
            continue
        seen.add(iid)
        name, mt = items[iid]
        if name not in names:
            continue
        if mt in _XHTML_TYPES or (not mt and name.lower().endswith(_XHTML_EXT)):
            docs.append(name)
        elif mt == "application/x-dtbncx+xml" or name.lower().endswith(".ncx"):
            ncx = name

    for el in root.iter():
        tag = el.tag.rsplit("}", 1)[-1]
        if tag == "title" and not title and el.text:
            title = el.text.strip()
        elif tag == "language" and not lang and el.text:
            lang = el.text.strip()
    return docs, ncx, title, lang


def _check_drm(zf: zipfile.ZipFile) -> None:
    if "META-INF/encryption.xml" not in zf.namelist():
        return
    try:
        xml = zf.read("META-INF/encryption.xml").decode("utf-8", "ignore")
    except KeyError:
        return
    if re.search(r'URI="[^"]+\.(x?html?|xml)"', xml, re.I):
        raise EpubError("Yeh EPUB DRM-protected hai, isko translate nahi kiya ja sakta.")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def analyse(path: str) -> Analysis:
    """Parse the EPUB and collect every translatable text node (CPU bound – run in a thread)."""
    if not zipfile.is_zipfile(path):
        raise EpubError("File valid EPUB nahi hai (ZIP header missing).")
    try:
        zf = zipfile.ZipFile(path)
    except zipfile.BadZipFile as exc:
        raise EpubError("EPUB file corrupt hai.") from exc

    with zf:
        if zf.testzip() is not None:
            raise EpubError("EPUB ke andar corrupt entries hain.")
        _check_drm(zf)
        opf = _find_opf(zf)
        if not opf:
            raise EpubError("EPUB mein OPF package file nahi mila.")
        doc_names, ncx_name, title, lang = _manifest(zf, opf)
        if not doc_names:
            raise EpubError("EPUB mein koi text document (XHTML) nahi mila.")

        analysis = Analysis(path=path, title=title or os.path.splitext(os.path.basename(path))[0],
                            opf_name=opf, language=lang)
        for name in doc_names:
            try:
                raw = zf.read(name)
            except KeyError:
                continue
            soup = BeautifulSoup(raw, "html.parser")
            nodes = _collect_nodes(soup)
            if nodes:
                analysis.docs.append(DocInfo(name, soup, nodes))
        if ncx_name:
            try:
                # NCX is real XML (case-sensitive tags) – use the XML parser
                soup = BeautifulSoup(zf.read(ncx_name), "xml")
                nodes = _ncx_nodes(soup)
                if nodes:
                    analysis.docs.append(DocInfo(ncx_name, soup, nodes, is_ncx=True))
            except KeyError:
                pass

    analysis.total_nodes = sum(len(d.nodes) for d in analysis.docs)
    analysis.total_chars = sum(len(str(n).strip()) for d in analysis.docs for n in d.nodes)
    if analysis.total_nodes == 0:
        raise EpubError("Is EPUB mein koi translatable text nahi mila (image-only book?).")
    return analysis


def make_batches(texts: List[str], max_items: int, max_chars: int) -> List[List[int]]:
    batches: List[List[int]] = []
    cur: List[int] = []
    chars = 0
    for i, t in enumerate(texts):
        if cur and (len(cur) >= max_items or chars + len(t) > max_chars):
            batches.append(cur)
            cur, chars = [], 0
        cur.append(i)
        chars += len(t)
    if cur:
        batches.append(cur)
    return batches


def _preserve_ws(original: str, translated: str) -> str:
    lead = original[: len(original) - len(original.lstrip())]
    trail = original[len(original.rstrip()):]
    return f"{lead}{translated.strip()}{trail}"


def _apply(analysis: Analysis, translated: List[str]) -> None:
    i = 0
    for doc in analysis.docs:
        for node in doc.nodes:
            t = translated[i]
            i += 1
            if t and t != str(node):
                try:
                    node.replace_with(NavigableString(_preserve_ws(str(node), t)))
                except Exception:  # node detached – ignore
                    pass


def _serialize(doc: DocInfo) -> bytes:
    if doc.is_ncx:
        return str(doc.soup).encode("utf-8")          # lxml-xml keeps the declaration + case
    return doc.soup.decode(formatter="minimal").encode("utf-8")


def _patch_opf(raw: bytes, lang: str) -> bytes:
    """Update <dc:language> so readers pick the right fonts / hyphenation."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw
    new, n = re.subn(r"(<dc:language[^>]*>)[^<]*(</dc:language>)", rf"\g<1>{lang}\g<2>", text, count=1)
    return new.encode("utf-8") if n else raw


def write_translated(analysis: Analysis, lang: str, out_path: str) -> None:
    """Copy the original ZIP, swapping translated documents in.  Atomic on success."""
    replaced: Dict[str, bytes] = {d.name: _serialize(d) for d in analysis.docs}
    tmp = out_path + ".part"
    with zipfile.ZipFile(analysis.path) as src, zipfile.ZipFile(tmp, "w") as dst:
        infos = src.infolist()
        # 'mimetype' must be the first entry and stored uncompressed (EPUB spec)
        dst.writestr(zipfile.ZipInfo("mimetype"), b"application/epub+zip", compress_type=zipfile.ZIP_STORED)
        for info in infos:
            if info.filename == "mimetype" or info.filename.endswith("/"):
                continue
            if info.filename in replaced:
                dst.writestr(info.filename, replaced[info.filename], compress_type=zipfile.ZIP_DEFLATED)
            elif info.filename == analysis.opf_name:
                dst.writestr(info.filename, _patch_opf(src.read(info), lang), compress_type=zipfile.ZIP_DEFLATED)
            else:
                dst.writestr(info, src.read(info), compress_type=info.compress_type)
    os.replace(tmp, out_path)


TranslateFn = Callable[[List[str], str, int], Awaitable[Tuple[List[str], int]]]
ProgressFn = Callable[[int, int], Awaitable[None]]           # (done_chars, failed_segments)


async def translate(
    analysis: Analysis,
    lang: str,
    translate_batch: TranslateFn,
    on_progress: Optional[ProgressFn] = None,
    cancel: Optional[asyncio.Event] = None,
    out_dir: Optional[str] = None,
) -> str:
    """
    Translate every collected node through `translate_batch` and write the
    new EPUB.  Returns the output path.  Raises asyncio.CancelledError when
    `cancel` is set.
    """
    texts = analysis.texts()
    results: List[str] = list(texts)
    batches = make_batches(texts, settings.batch_items, settings.batch_chars)
    sem = asyncio.Semaphore(max(1, settings.parallel_batches))
    done_chars = 0
    failed = 0
    lock = asyncio.Lock()

    async def run(bi: int, idxs: List[int]) -> None:
        nonlocal done_chars, failed
        if cancel and cancel.is_set():
            raise asyncio.CancelledError
        async with sem:
            if cancel and cancel.is_set():
                raise asyncio.CancelledError
            out, nf = await translate_batch([texts[i] for i in idxs], lang, bi)
        if cancel and cancel.is_set():
            raise asyncio.CancelledError
        for i, t in zip(idxs, out):
            results[i] = t
        async with lock:
            done_chars += sum(len(texts[i].strip()) for i in idxs)
            failed += nf
        if on_progress:
            try:
                await on_progress(done_chars, failed)
            except Exception:  # never let UI errors break the job
                pass

    tasks = [asyncio.create_task(run(bi, b)) for bi, b in enumerate(batches)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    if cancel and cancel.is_set():
        raise asyncio.CancelledError

    _apply(analysis, results)
    out_dir = out_dir or os.path.dirname(analysis.path) or "."
    safe_title = re.sub(r"[^\w\s.-]", "", analysis.title, flags=re.UNICODE).strip()[:60] or "book"
    fd, out_path = tempfile.mkstemp(prefix=f"{safe_title}_{lang}_", suffix=".epub", dir=out_dir)
    os.close(fd)
    await asyncio.to_thread(write_translated, analysis, lang, out_path)
    analysis.failed_segments = failed  # type: ignore[attr-defined]
    return out_path


def cleanup(*paths: Optional[str]) -> None:
    for p in paths:
        if not p:
            continue
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.exists(p):
                os.remove(p)
        except OSError:
            pass


# =============================================================================
# Worker pool (ONLY place that does network translation calls — via workers)
# =============================================================================
"""
Worker pool – talks to the translation workers (app.py) on Render / Vercel.

* Least-loaded routing: each batch goes to the healthy worker with the fewest
  in-flight requests (ties broken by measured latency).
* Failover: on any error the batch is retried on the next worker; a failing
  worker is put on cooldown so it does not slow down every batch.
* Keep-alive: pings every worker periodically so free-tier hosts stay awake.
* Never raises into the caller except `NoWorkersError` when the pool is empty.
"""

class NoWorkersError(RuntimeError):
    """Raised when there is no worker at all to send a batch to."""


class WorkerPool:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.urls: List[str] = list(dict.fromkeys(store.workers() + settings.seed_workers))
        for url in settings.seed_workers:
            store.add_worker(url)
        self.fail_at: Dict[str, float] = {}                    # url -> last failure ts
        self.inflight: Dict[str, int] = {}                     # url -> concurrent requests
        self.latency: Dict[str, float] = {}                    # url -> EWMA seconds
        self.stats: Dict[str, Dict[str, int]] = {}             # url -> {"ok", "fail"}
        self.last_health: Dict[str, Tuple[bool, str]] = {}     # url -> (ok, info)
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------ lifecycle
    def __len__(self) -> int:
        return len(self.urls)

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=settings.worker_timeout_s),
                connector=aiohttp.TCPConnector(limit=64, ttl_dns_cache=300),
                headers=self._headers(),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _headers() -> dict:
        return {"X-Worker-Secret": settings.worker_secret} if settings.worker_secret else {}

    # ----------------------------------------------------------- management
    @staticmethod
    def normalize(url: str) -> str:
        url = url.strip().rstrip("/")
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        return url

    def add(self, url: str) -> bool:
        url = self.normalize(url)
        if url in self.urls:
            return False
        self.urls.append(url)
        self.store.add_worker(url)
        return True

    def remove(self, url: str) -> bool:
        if url not in self.urls:
            return False
        self.urls.remove(url)
        self.store.remove_worker(url)
        for d in (self.fail_at, self.inflight, self.latency, self.stats, self.last_health):
            d.pop(url, None)  # type: ignore[arg-type]
        return True

    def healthy(self) -> List[str]:
        now = time.time()
        return [u for u in self.urls if now - self.fail_at.get(u, 0.0) > settings.worker_cooldown_s]

    def _mark(self, url: str, ok: bool, took: Optional[float] = None) -> None:
        s = self.stats.setdefault(url, {"ok": 0, "fail": 0})
        if ok:
            s["ok"] += 1
            self.fail_at.pop(url, None)
            if took is not None:
                prev = self.latency.get(url)
                self.latency[url] = took if prev is None else prev * 0.7 + took * 0.3
        else:
            s["fail"] += 1
            self.fail_at[url] = time.time()

    def _ordered(self, hint: int) -> List[str]:
        """Healthy workers first (least loaded, then fastest), unhealthy as a last resort."""
        healthy = self.healthy()
        if not healthy and not self.urls:
            raise NoWorkersError("No translation workers configured")
        candidates = healthy or list(self.urls)
        candidates.sort(key=lambda u: (self.inflight.get(u, 0), self.latency.get(u, 0.5)))
        if len(candidates) > 1 and candidates[0] and self.inflight.get(candidates[0], 0) == \
                self.inflight.get(candidates[1], 0):
            # spread evenly among equally-loaded workers
            k = hint % len(candidates)
            candidates = candidates[k:] + candidates[:k]
        rest = [u for u in self.urls if u not in candidates]
        return candidates + rest

    # ----------------------------------------------------------------- health
    async def check(self, url: str) -> Tuple[bool, str]:
        """Return (ok, human-readable info)."""
        url = self.normalize(url)
        try:
            session = await self.session()
            t0 = time.perf_counter()
            async with session.get(f"{url}/health", timeout=aiohttp.ClientTimeout(total=25)) as r:
                took = time.perf_counter() - t0
                if r.status != 200:
                    info = (False, f"HTTP {r.status}")
                else:
                    data = await r.json(content_type=None)
                    info = (True, f"v{data.get('version', '?')} • load {data.get('load', 0)} • "
                                  f"{int(took * 1000)}ms")
                    self.latency[url] = took if url not in self.latency else self.latency[url] * 0.7 + took * 0.3
        except asyncio.TimeoutError:
            info = (False, "timeout")
        except aiohttp.ClientError as exc:
            info = (False, type(exc).__name__)
        except Exception as exc:  # pragma: no cover
            info = (False, str(exc)[:50])
        self.last_health[url] = info
        if info[0]:
            self.fail_at.pop(url, None)
        else:
            self.fail_at[url] = time.time()
        return info

    async def check_all(self) -> Dict[str, Tuple[bool, str]]:
        results = await asyncio.gather(*(self.check(u) for u in list(self.urls)), return_exceptions=True)
        out: Dict[str, Tuple[bool, str]] = {}
        for url, res in zip(list(self.urls), results):
            out[url] = res if isinstance(res, tuple) else (False, "error")
        return out

    async def keepalive_loop(self) -> None:
        """Ping workers forever (free-tier hosts sleep after ~15 min idle)."""
        while True:
            try:
                if self.urls:
                    await self.check_all()
            except Exception as exc:  # pragma: no cover
                log.debug("keepalive error: %s", exc)
            await asyncio.sleep(max(60.0, settings.keepalive_interval_s))

    # -------------------------------------------------------------- translate
    async def translate(self, texts: List[str], lang: str, hint: int = 0) -> Tuple[List[str], int]:
        """
        Translate a batch with failover.  Returns (translated, failed_count).
        As a last resort the original texts are returned with failed=len(texts).
        """
        if not texts:
            return [], 0
        order = self._ordered(hint)
        session = await self.session()
        payload = {"text_list": texts, "lang": lang, "source": "auto"}

        for url in order:
            for attempt in range(2):
                self.inflight[url] = self.inflight.get(url, 0) + 1
                t0 = time.perf_counter()
                try:
                    async with session.post(f"{url}/translate", json=payload) as resp:
                        if resp.status == 401:
                            log.error("Worker %s rejected WORKER_SECRET", url)
                            self._mark(url, False)
                            break
                        if resp.status == 429:            # busy – try another worker right away
                            log.info("Worker %s busy", url)
                            break
                        if resp.status != 200:
                            raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=resp.status)
                        data = await resp.json(content_type=None)
                    out = data.get("translated") if isinstance(data, dict) else None
                    if data.get("success") and isinstance(out, list) and len(out) == len(texts):
                        self._mark(url, True, time.perf_counter() - t0)
                        return [str(t) if t is not None else o for t, o in zip(out, texts)], int(data.get("failed", 0))
                    raise RuntimeError(str(data.get("error", "bad response"))[:80])
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError, ValueError) as exc:
                    log.warning("Worker %s failed (%s) attempt %d", url, exc, attempt + 1)
                    await asyncio.sleep(0.8 * (attempt + 1))
                finally:
                    self.inflight[url] = max(0, self.inflight.get(url, 1) - 1)
            else:
                self._mark(url, False)
        log.error("All workers failed for a batch of %d texts", len(texts))
        return list(texts), len(texts)


# =============================================================================
# Job manager (queue + runner)
# =============================================================================
"""
Job manager – pending confirmations, a bounded queue and N concurrent runners.

Flow
----
prepare()  download done -> analyse (thread) -> Pending (waits for user tap)
enqueue()  user confirmed & paid          -> Job in queue
runner     picks Job -> epub_engine.translate -> callbacks -> cleanup/refund
"""

@dataclass
class Progress:
    total: int = 0
    done: int = 0
    failed_segments: int = 0
    started: float = 0.0

    @property
    def pct(self) -> int:
        return min(100, int(self.done * 100 / self.total)) if self.total else 0

    @property
    def elapsed(self) -> int:
        return int(time.time() - self.started) if self.started else 0

    @property
    def eta(self) -> Optional[int]:
        if not self.done or not self.started:
            return None
        rate = self.done / max(0.001, time.time() - self.started)
        return int((self.total - self.done) / rate) if rate > 0 else None


@dataclass
class Pending:
    token: str
    user_id: int
    chat_id: int
    msg_id: int
    path: str
    file_name: str
    lang: str
    analysis: Analysis
    created: float = field(default_factory=time.time)


@dataclass
class Job:
    id: int
    user_id: int
    chat_id: int
    msg_id: int
    path: str
    file_name: str
    title: str
    lang: str
    analysis: Analysis
    breakdown: dict
    progress: Progress = field(default_factory=Progress)
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    out_path: Optional[str] = None


Callback = Callable[..., Awaitable[None]]


class JobManager:
    def __init__(self, store: Store, pool: WorkerPool) -> None:
        self.store = store
        self.pool = pool
        self.queue: "asyncio.Queue[Job]" = asyncio.Queue()
        self.pending: Dict[str, Pending] = {}
        self.running: Dict[int, Job] = {}            # job id -> Job
        self._queued: Dict[int, Job] = {}            # job id -> Job (waiting)
        self._runners: List[asyncio.Task] = []
        self._janitor: Optional[asyncio.Task] = None
        self.on_start: Optional[Callback] = None
        self.on_progress: Optional[Callback] = None
        self.on_finish: Optional[Callback] = None
        self.on_expire: Optional[Callback] = None

    # --------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self.store.reset_stale_jobs()
        for i in range(max(1, settings.max_concurrent_jobs)):
            self._runners.append(asyncio.create_task(self._runner(i), name=f"runner-{i}"))
        self._janitor = asyncio.create_task(self._janitor_loop(), name="janitor")

    async def stop(self) -> None:
        for job in list(self.running.values()):
            job.cancel.set()
        for t in self._runners + ([self._janitor] if self._janitor else []):
            t.cancel()
        await asyncio.gather(*self._runners, return_exceptions=True)
        for p in list(self.pending.values()):
            cleanup(p.path)
        for job in list(self._queued.values()):
            self.store.refund(job.user_id, job.breakdown)
            self.store.update_job(job.id, "cancelled", "shutdown")
            cleanup(job.path)

    # ----------------------------------------------------------------- queries
    def has_active(self, uid: int) -> bool:
        return any(j.user_id == uid for j in self.running.values()) or \
            any(j.user_id == uid for j in self._queued.values())

    def user_pending(self, uid: int) -> Optional[Pending]:
        return next((p for p in self.pending.values() if p.user_id == uid), None)

    def user_job(self, uid: int) -> Optional[Job]:
        for j in list(self.running.values()) + list(self._queued.values()):
            if j.user_id == uid:
                return j
        return None

    def queued_job(self, jid: int) -> Optional[Job]:
        return self._queued.get(jid)

    def position(self, job: Job) -> int:
        """1-based position in the waiting queue, 0 if running."""
        if job.id in self.running:
            return 0
        ids = [j.id for j in self._queued.values()]
        return ids.index(job.id) + 1 if job.id in ids else 0

    # ------------------------------------------------------------------ pending
    async def prepare(self, uid: int, chat_id: int, path: str, file_name: str, lang: str, msg_id: int) -> Pending:
        analysis = await asyncio.to_thread(analyse, path)
        token = secrets.token_urlsafe(8)
        p = Pending(token, uid, chat_id, msg_id, path, file_name, lang, analysis)
        self.pending[token] = p
        return p

    def drop_pending(self, token: str) -> Optional[Pending]:
        p = self.pending.pop(token, None)
        if p:
            cleanup(p.path)
        return p

    # ------------------------------------------------------------------- queue
    async def enqueue(self, p: Pending, breakdown: dict) -> Job:
        self.pending.pop(p.token, None)
        jid = self.store.create_job(p.user_id, p.file_name, p.lang, p.analysis.total_chars)
        job = Job(jid, p.user_id, p.chat_id, p.msg_id, p.path, p.file_name, p.analysis.title, p.lang,
                  p.analysis, breakdown)
        job.progress.total = p.analysis.total_chars
        self._queued[jid] = job
        await self.queue.put(job)
        return job

    def cancel_job(self, jid: int) -> Optional[Job]:
        job = self.running.get(jid) or self._queued.get(jid)
        if job:
            job.cancel.set()
        return job

    def cancel_user(self, uid: int) -> Optional[Job]:
        job = self.user_job(uid)
        if job:
            job.cancel.set()
        return job

    def cancel_all(self) -> int:
        jobs = list(self.running.values()) + list(self._queued.values())
        for j in jobs:
            j.cancel.set()
        return len(jobs)

    # ------------------------------------------------------------------ runner
    async def _runner(self, idx: int) -> None:
        while True:
            job = await self.queue.get()
            try:
                self._queued.pop(job.id, None)
                if job.cancel.is_set():
                    await self._finish(job, None, "cancelled")
                    continue
                self.running[job.id] = job
                await self._run(job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover – last line of defence
                log.exception("runner %d crashed on job %s", idx, job.id)
                await self._finish(job, None, f"internal error ({type(exc).__name__})")
            finally:
                self.running.pop(job.id, None)
                self.queue.task_done()

    async def _run(self, job: Job) -> None:
        job.progress.started = time.time()
        self.store.update_job(job.id, "running")
        if self.on_start:
            await self._safe(self.on_start, job)

        async def progress(done: int, failed: int) -> None:
            job.progress.done = done
            job.progress.failed_segments = failed
            if self.on_progress:
                await self._safe(self.on_progress, job)

        try:
            out = await translate(job.analysis, job.lang, self.pool.translate, progress,
                                              job.cancel, settings.download_dir)
        except asyncio.CancelledError:
            if job.cancel.is_set():
                return await self._finish(job, None, "cancelled")
            raise
        except NoWorkersError:
            return await self._finish(job, None, "Koi translation worker online nahi hai.")
        except EpubError as exc:
            return await self._finish(job, None, str(exc))
        except Exception as exc:
            log.exception("job %s failed", job.id)
            return await self._finish(job, None, f"{type(exc).__name__}: {str(exc)[:120]}")

        job.progress.done = job.progress.total
        job.progress.failed_segments = getattr(job.analysis, "failed_segments", 0)
        if job.progress.failed_segments and job.progress.failed_segments >= job.analysis.total_nodes:
            cleanup(out)
            return await self._finish(job, None, "Workers se koi translation nahi mili (sab batches fail).")
        await self._finish(job, out, None)

    async def _finish(self, job: Job, out_path: Optional[str], error: Optional[str]) -> None:
        status = "cancelled" if error == "cancelled" else ("failed" if error else "done")
        if status != "done":
            self.store.refund(job.user_id, job.breakdown)
        self.store.update_job(job.id, status, error)
        job.out_path = out_path
        if self.on_finish:
            await self._safe(self.on_finish, job, out_path, error)
        cleanup(job.path, out_path)

    @staticmethod
    async def _safe(cb: Callback, *args) -> None:
        try:
            await cb(*args)
        except Exception as exc:
            log.warning("callback %s failed: %s", getattr(cb, "__name__", cb), exc)

    # ----------------------------------------------------------------- janitor
    async def _janitor_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                now = time.time()
                for token, p in list(self.pending.items()):
                    if now - p.created > settings.pending_ttl_s:
                        self.drop_pending(token)
                        if self.on_expire:
                            await self._safe(self.on_expire, p)
                self.store.expire_old_pending(hours=24)
                # stray files older than 6h in the download dir
                d = settings.download_dir
                if os.path.isdir(d):
                    keep = {p.path for p in self.pending.values()} | {j.path for j in self.running.values()} | \
                           {j.path for j in self._queued.values()}
                    for f in os.listdir(d):
                        fp = os.path.join(d, f)
                        try:
                            if fp not in keep and now - os.path.getmtime(fp) > 6 * 3600:
                                cleanup(fp)
                        except OSError:
                            pass
            except Exception as exc:  # pragma: no cover
                log.debug("janitor: %s", exc)


# =============================================================================
# Telegram UI (texts & keyboards)
# =============================================================================
"""
UI layer – texts and keyboards.

Design rule: every screen shows only the buttons that are useful *right now*
(2–6 buttons max).  Deeper options live one tap away behind a menu button.
All texts use HTML parse mode; anything user-supplied goes through esc().
"""

LANG_PAGE = 8
CREDIT_PRESETS = (2, 4, 6)


def esc(s: object) -> str:
    return html.escape(str(s or ""), quote=False)


def _bar(pct: int, width: int = 12) -> str:
    filled = int(round(width * max(0, min(100, pct)) / 100))
    return "█" * filled + "░" * (width - filled)


def _dur(sec: Optional[int]) -> str:
    if sec is None:
        return "…"
    sec = max(0, int(sec))
    return f"{sec // 60}m {sec % 60:02d}s" if sec >= 60 else f"{sec}s"


# --------------------------------------------------------------------------- #
# Reply keyboard (persistent, 3 buttons)
# --------------------------------------------------------------------------- #
BTN_PLAN, BTN_LANG, BTN_BUY, BTN_ADMIN = "👤 My Plan", "🌐 Language", "💳 Buy", "⚙️ Admin"


def main_keyboard(admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(BTN_PLAN), KeyboardButton(BTN_LANG)], [KeyboardButton(BTN_BUY)]]
    if admin:
        rows[1].append(KeyboardButton(BTN_ADMIN))
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)


# --------------------------------------------------------------------------- #
# Texts
# --------------------------------------------------------------------------- #
def welcome_text(name: str) -> str:
    return (
        f"👋 Namaste <b>{esc(name)}</b>!\n\n"
        "Main EPUB books ko aapki bhasha mein translate karta hoon — images, cover, "
        "CSS aur chapters bilkul same rehte hain.\n\n"
        f"🆓 <b>Free:</b> {fmt_chars(settings.free_daily_chars)} characters har din\n"
        f"⭐ <b>Unlimited:</b> ₹{settings.unlimited_price_inr} / {settings.unlimited_days} din\n"
        f"💎 <b>Credits:</b> ₹{settings.credit_price_per_million_inr} per 1M characters (jitna chahiye)\n\n"
        "📎 Shuru karne ke liye bas apni <b>.epub</b> file bhejein."
    )


def help_text() -> str:
    return (
        "📖 <b>Kaise use karein</b>\n\n"
        "1. 🌐 Language chunein (ek baar; baad mein file ke saath bhi badal sakte hain)\n"
        "2. 📎 <b>.epub</b> file bhejein\n"
        "3. ✅ Characters aur price dekh kar <b>Translate</b> dabayein\n"
        "4. ⏳ Progress dikhega, phir translated EPUB mil jayegi\n\n"
        "<b>Commands</b>\n"
        "/plan – balance & history\n"
        "/lang – language badlein\n"
        "/buy – unlimited ya credits lein\n"
        "/cancel – chal raha job rokein\n\n"
        f"{('📞 Support: ' + esc(settings.support_contact)) if settings.support_contact else ''}"
    ).strip()


def plan_text(a: dict, u: dict) -> str:
    if a["unlimited"]:
        plan = f"⭐ <b>Unlimited</b> (till {a['unlimited_until']})"
    else:
        plan = "🆓 <b>Free</b>" if not a["credits"] else "💎 <b>Credits</b>"
    lines = [
        f"👤 <b>Your plan:</b> {plan}",
        f"🌐 Language: <b>{lang_name(u['lang'])}</b>",
        "",
    ]
    if not a["unlimited"]:
        lines.append(f"🆓 Free today: <b>{fmt_chars(a['free_left'])}</b> / {fmt_chars(a['free_daily'])}")
        lines.append(f"💎 Credits: <b>{fmt_chars(a['credits'])}</b>")
    lines.append(f"📚 Translated: {u['total_files']} files • {fmt_chars(u['total_chars'])} chars")
    return "\n".join(lines)


def confirm_text(title: str, file_name: str, chars: int, nodes: int, lang: str, a: dict) -> str:
    lines = [
        f"📚 <b>{esc(title or file_name)}</b>",
        f"📝 {fmt_chars(chars)} characters • {nodes:,} segments",
        f"🌐 Translate to: <b>{lang_name(lang)}</b>",
        "",
    ]
    if a["unlimited"]:
        lines.append("⭐ Unlimited plan — <b>free</b>")
    else:
        from_free = min(a["free_left"], chars)
        from_cred = chars - from_free
        if from_cred <= a["credits"]:
            cost = []
            if from_free:
                cost.append(f"{fmt_chars(from_free)} free")
            if from_cred:
                cost.append(f"{fmt_chars(from_cred)} credits")
            lines.append("💰 Cost: <b>" + " + ".join(cost) + "</b>")
            left_free, left_cred = a["free_left"] - from_free, a["credits"] - from_cred
            lines.append(f"↳ Baad mein: {fmt_chars(left_free)} free • {fmt_chars(left_cred)} credits")
        else:
            short = chars - a["total"]
            lines.append(f"⚠️ Balance kam hai — <b>{fmt_chars(short)}</b> characters aur chahiye.")
            lines.append(f"Available: {fmt_chars(a['free_left'])} free + {fmt_chars(a['credits'])} credits")
    return "\n".join(lines)


def progress_text(title: str, lang: str, pct: int, done: int, total: int, elapsed: int,
                  eta: Optional[int], healthy: int, workers: int) -> str:
    return (
        f"⏳ <b>{esc(title)}</b> → {lang_name(lang)}\n\n"
        f"<code>{_bar(pct)}</code> <b>{pct}%</b>\n"
        f"{fmt_chars(done)} / {fmt_chars(total)} chars\n"
        f"⏱ {_dur(elapsed)} • ETA {_dur(eta)}\n"
        f"🖥 Workers {healthy}/{workers}"
    )


def buy_menu_text(a: dict) -> str:
    bal = ("⭐ Unlimited active till " + str(a["unlimited_until"])) if a["unlimited"] else \
        f"Balance: {fmt_chars(a['free_left'])} free today + {fmt_chars(a['credits'])} credits"
    return (
        "💳 <b>Plans</b>\n\n"
        f"⭐ <b>Unlimited</b> — ₹{settings.unlimited_price_inr} / {settings.unlimited_days} din, koi limit nahi\n"
        f"💎 <b>Credits</b> — ₹{settings.credit_price_per_million_inr} per 1M characters, kabhi expire nahi hote\n\n"
        f"{bal}"
    )


# --------------------------------------------------------------------------- #
# Inline keyboards
# --------------------------------------------------------------------------- #
def back_keyboard(cb: str, label: str = "« Back") -> KB:
    return KB([[B(label, callback_data=cb)]])


def cancel_keyboard(cb: str = "cancel") -> KB:
    return KB([[B("✖️ Cancel", callback_data=cb)]])


def lang_keyboard(current: str, page: int = 0, back: Optional[str] = None) -> KB:
    codes = list(LANGUAGES)
    pages = max(1, -(-len(codes) // LANG_PAGE))
    page = max(0, min(page, pages - 1))
    chunk = codes[page * LANG_PAGE:(page + 1) * LANG_PAGE]
    back_arg = back or "-"
    rows: List[List[B]] = []
    for i in range(0, len(chunk), 2):
        row = []
        for c in chunk[i:i + 2]:
            mark = "✅ " if c == current else ""
            row.append(B(f"{mark}{LANGUAGES[c]}", callback_data=f"lang:{c}:{page}:{back_arg}"))
        rows.append(row)
    nav = []
    if page > 0:
        nav.append(B("‹ Prev", callback_data=f"langpg:{page - 1}:{back_arg}"))
    nav.append(B(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(B("Next ›", callback_data=f"langpg:{page + 1}:{back_arg}"))
    rows.append(nav)
    if back:
        rows.append([B("« Back", callback_data=back)])
    return KB(rows)


def confirm_keyboard(token: str, enough: bool, short_millions: int = 0) -> KB:
    if enough:
        rows = [[B("▶️ Translate", callback_data=f"job:start:{token}")],
                [B("🌐 Language", callback_data=f"job:lang:{token}"),
                 B("✖️ Cancel", callback_data=f"job:cancel:{token}")]]
    else:
        m = max(settings.min_credit_millions, min(short_millions, settings.max_credit_millions))
        rows = [[B(f"💎 Buy {m}M credits — ₹{credits_price(m)}", callback_data=f"buy:credits:{m}")],
                [B(f"⭐ Unlimited — ₹{settings.unlimited_price_inr}", callback_data="buy:unlimited")],
                [B("🔄 Re-check", callback_data=f"job:view:{token}"),
                 B("✖️ Cancel", callback_data=f"job:cancel:{token}")]]
    return KB(rows)


def progress_keyboard(job_id: int) -> KB:
    return KB([[B("⏹ Stop", callback_data=f"job:stop:{job_id}")]])


def plan_keyboard() -> KB:
    return KB([[B("💳 Buy", callback_data="buy:menu"), B("📜 History", callback_data="me:history")]])


def buy_menu_keyboard() -> KB:
    rows = [[B(f"⭐ Unlimited — ₹{settings.unlimited_price_inr} / {settings.unlimited_days} din",
               callback_data="buy:unlimited")],
            [B(f"💎 {m}M — ₹{credits_price(m)}", callback_data=f"buy:credits:{m}") for m in CREDIT_PRESETS],
            [B("✏️ Custom amount", callback_data="buy:custom")]]
    return KB(rows)


def pay_method_keyboard(kind: str, millions: int, rzp: bool, upi: bool) -> KB:
    row = []
    if rzp:
        row.append(B("💳 Pay online", callback_data=f"pay:rzp:{kind}:{millions}"))
    if upi:
        row.append(B("📲 UPI", callback_data=f"pay:upi:{kind}:{millions}"))
    rows = [row] if row else [[B("📞 Contact admin", callback_data="pay:contact")]]
    rows.append([B("« Back", callback_data="buy:menu")])
    return KB(rows)


def rzp_keyboard(url: str, pid: int) -> KB:
    return KB([[B("💳 Open payment page", url=url)],
               [B("✅ Verify payment", callback_data=f"pay:check:{pid}"),
                B("✖️ Cancel", callback_data=f"pay:abort:{pid}")]])


def upi_keyboard(pid: int) -> KB:
    # Telegram inline buttons only allow http(s)/tg links, so UPI details are shown as copyable text.
    return KB([[B("✖️ Cancel", callback_data=f"pay:abort:{pid}")]])


# ---- admin ------------------------------------------------------------------
def admin_menu_keyboard() -> KB:
    return KB([[B("📊 Stats", callback_data="adm:stats"), B("🖥 Workers", callback_data="adm:workers")],
               [B("💰 Payments", callback_data="adm:payments"), B("📣 Broadcast", callback_data="adm:bcast")],
               [B("🧹 Maintenance", callback_data="adm:maint")]])


def admin_workers_keyboard(urls: List[str]) -> KB:
    row = [B("➕ Add", callback_data="adm:w:add")]
    if urls:
        row += [B("➖ Remove", callback_data="adm:w:del"), B("🔄 Ping", callback_data="adm:w:ping")]
    return KB([row, [B("« Back", callback_data="adm:menu")]])


def admin_del_keyboard(urls: List[str]) -> KB:
    rows = [[B(f"🗑 {i + 1}. {u.replace('https://', '')[:30]}", callback_data=f"adm:w:rm:{i}")]
            for i, u in enumerate(urls)]
    rows.append([B("« Back", callback_data="adm:workers")])
    return KB(rows)


def admin_review_keyboard(pid: int) -> KB:
    return KB([[B("✅ Approve", callback_data=f"adm:pay:ok:{pid}"),
                B("❌ Reject", callback_data=f"adm:pay:no:{pid}")],
               [B("« Back", callback_data="adm:menu")]])


def admin_maint_keyboard() -> KB:
    return KB([[B("♻️ Reset worker cooldowns", callback_data="adm:m:cooldown")],
               [B("⏹ Cancel all jobs", callback_data="adm:m:killjobs")],
               [B("🧾 Expire old payments", callback_data="adm:m:expire")],
               [B("« Back", callback_data="adm:menu")]])


# =============================================================================
# Telegram bot (handlers, admin, entrypoint)
# =============================================================================

# --------------------------------------------------------------------------- #
# Bootstrap
# --------------------------------------------------------------------------- #
settings.validate()

_handlers: list = [logging.StreamHandler()]
try:
    _handlers.append(logging.handlers.RotatingFileHandler(settings.log_file, maxBytes=5_000_000, backupCount=3,
                                                          encoding="utf-8"))
except OSError:
    pass
logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s", handlers=_handlers)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

store = Store(settings.db_file)
store.migrate_legacy_json(settings.legacy_json)
pool = WorkerPool(store)
jobs = JobManager(store, pool)
rzp = Razorpay()

app = Client("epub_translator", api_id=settings.api_id, api_hash=settings.api_hash, bot_token=settings.bot_token,
             workdir=settings.data_dir, parse_mode=enums.ParseMode.HTML, sleep_threshold=30)

state: Dict[int, dict] = {}          # transient conversational state: user_id -> {"mode": ..}
_last_edit: Dict[int, float] = {}    # job_id -> last progress edit timestamp

ADMIN_CMDS = ["admin", "grant", "credits", "revoke", "ban", "unban", "user", "stats", "workers"]
USER_CMDS = ["start", "help", "plan", "lang", "buy", "pay", "cancel"]


def is_admin(uid: int) -> bool:
    return settings.is_admin(uid)


admin_only = filters.create(lambda _, __, m: bool(m.from_user) and is_admin(m.from_user.id))


# --------------------------------------------------------------------------- #
# Safe Telegram helpers – never raise into handlers
# --------------------------------------------------------------------------- #
async def safe_edit(chat_id: int, msg_id: int, text: str, reply_markup=None) -> bool:
    try:
        await app.edit_message_text(chat_id, msg_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
        return True
    except MessageNotModified:
        return True
    except FloodWait as fw:
        await asyncio.sleep(min(int(fw.value), 30))
        return False
    except (RPCError, Exception) as exc:  # noqa: BLE001
        log.debug("edit failed: %s", exc)
        return False


async def safe_send(chat_id: int, text: str, reply_markup=None) -> Optional[Message]:
    for _ in range(2):
        try:
            return await app.send_message(chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
        except FloodWait as fw:
            await asyncio.sleep(min(int(fw.value), 30))
        except Exception as exc:  # noqa: BLE001  (blocked bot, deleted account, …)
            log.debug("send failed to %s: %s", chat_id, exc)
            return None
    return None


async def reply(m: Message, text: str, reply_markup=None) -> Optional[Message]:
    try:
        return await m.reply_text(text, reply_markup=reply_markup, disable_web_page_preview=True, quote=True)
    except FloodWait as fw:
        await asyncio.sleep(min(int(fw.value), 30))
        return await safe_send(m.chat.id, text, reply_markup)
    except Exception as exc:  # noqa: BLE001
        log.debug("reply failed: %s", exc)
        return None


async def answer(cq: CallbackQuery, text: str = "", alert: bool = False) -> None:
    try:
        await cq.answer(text[:200], show_alert=alert)
    except Exception:  # noqa: BLE001
        pass


async def edit_cq(cq: CallbackQuery, text: str, markup=None) -> None:
    if cq.message:
        await safe_edit(cq.message.chat.id, cq.message.id, text, markup)


async def notify_admins(text: str, reply_markup=None) -> None:
    for uid in settings.admins:
        await safe_send(uid, text, reply_markup)


def avail(uid: int) -> dict:
    return store.available_chars(uid, settings.free_daily_chars)


def touch(obj) -> Optional[dict]:
    u = obj.from_user
    if not u or u.is_bot:
        return None
    return store.touch_user(u.id, u.username, u.first_name)


def banned(uid: int) -> bool:
    u = store.get_user(uid)
    return bool(u and u.get("banned")) and not is_admin(uid)


# --------------------------------------------------------------------------- #
# Job callbacks  (JobManager -> Telegram)
# --------------------------------------------------------------------------- #
async def job_started(job: Job) -> None:
    _last_edit[job.id] = time.time()
    await safe_edit(job.chat_id, job.msg_id,
                    progress_text(job.title, job.lang, 0, 0, job.progress.total, 0, None,
                                     len(pool.healthy()), len(pool)),
                    progress_keyboard(job.id))


async def job_progress(job: Job) -> None:
    now = time.time()
    if now - _last_edit.get(job.id, 0) < 4:      # Telegram edit rate-limit friendly
        return
    _last_edit[job.id] = now
    p = job.progress
    await safe_edit(job.chat_id, job.msg_id,
                    progress_text(job.title, job.lang, p.pct, p.done, p.total, p.elapsed, p.eta,
                                     len(pool.healthy()), len(pool)),
                    progress_keyboard(job.id))


async def job_finished(job: Job, out_path: Optional[str], error: Optional[str]) -> None:
    _last_edit.pop(job.id, None)
    if error == "cancelled":
        return await safe_edit(job.chat_id, job.msg_id, "✖️ Translation cancelled. Characters refund ho gaye.")
    if error or not out_path:
        return await safe_edit(job.chat_id, job.msg_id,
                               f"❌ <b>Translation failed</b>\n{esc(error or 'Unknown error')}\n\n"
                               "Characters refund ho gaye. Thodi der baad dobara try karein.")

    await safe_edit(job.chat_id, job.msg_id, "✅ Translation complete — uploading…")
    p = job.progress
    warn = f"\n⚠️ {p.failed_segments} segments untranslated (worker errors)" if p.failed_segments else ""
    base = os.path.splitext(job.file_name)[0]
    caption = (f"📚 <b>{esc(job.title)}</b>\n🌐 {lang_name(job.lang)} • {fmt_chars(job.analysis.total_chars)} chars"
               f" • {p.elapsed}s{warn}")
    for attempt in range(3):
        try:
            await app.send_document(job.chat_id, out_path, caption=caption, file_name=f"{base}_{job.lang}.epub")
            break
        except FloodWait as fw:
            await asyncio.sleep(min(int(fw.value), 60))
        except Exception as exc:  # noqa: BLE001
            log.warning("upload attempt %d failed: %s", attempt + 1, exc)
            await asyncio.sleep(2)
    else:
        store.refund(job.user_id, job.breakdown)
        return await safe_edit(job.chat_id, job.msg_id, "❌ File upload failed. Characters refund ho gaye.")
    try:
        await app.delete_messages(job.chat_id, job.msg_id)
    except Exception:  # noqa: BLE001
        pass


async def pending_expired(p: Pending) -> None:
    await safe_edit(p.chat_id, p.msg_id, "⌛ Request expire ho gayi — file dobara bhejein.")


jobs.on_start, jobs.on_progress, jobs.on_finish, jobs.on_expire = job_started, job_progress, job_finished, pending_expired


# --------------------------------------------------------------------------- #
# User commands
# --------------------------------------------------------------------------- #
@app.on_message(filters.private & filters.command(["start", "help"]))
async def cmd_start(_, m: Message):
    if not touch(m):
        return
    uid = m.from_user.id
    state.pop(uid, None)
    text = welcome_text(m.from_user.first_name or "friend") if m.command[0] == "start" else help_text()
    await reply(m, text, main_keyboard(is_admin(uid)))


@app.on_message(filters.private & filters.command("plan"))
async def cmd_plan(_, m: Message):
    u = touch(m)
    if u:
        await reply(m, plan_text(avail(m.from_user.id), u), plan_keyboard())


@app.on_message(filters.private & filters.command("lang"))
async def cmd_lang(_, m: Message):
    u = touch(m)
    if u:
        await reply(m, "🌐 Target language chunein:", lang_keyboard(u["lang"]))


@app.on_message(filters.private & filters.command(["buy", "pay"]))
async def cmd_buy(_, m: Message):
    if touch(m):
        await reply(m, buy_menu_text(avail(m.from_user.id)), buy_menu_keyboard())


@app.on_message(filters.private & filters.command("cancel"))
async def cmd_cancel(_, m: Message):
    if not touch(m):
        return
    uid = m.from_user.id
    state.pop(uid, None)
    p = jobs.user_pending(uid)
    if p:
        jobs.drop_pending(p.token)
        await safe_edit(p.chat_id, p.msg_id, "✖️ Cancelled.")
    if jobs.cancel_user(uid):
        return await reply(m, "⏹ Job rok diya ja raha hai…")
    await reply(m, "✖️ Cancelled." if p else "Koi active job nahi hai.")


# --------------------------------------------------------------------------- #
# Admin commands
# --------------------------------------------------------------------------- #
@app.on_message(filters.private & filters.command("admin") & admin_only)
async def cmd_admin(_, m: Message):
    await reply(m, "⚙️ <b>Admin panel</b>", admin_menu_keyboard())


@app.on_message(filters.private & filters.command("stats") & admin_only)
async def cmd_stats(_, m: Message):
    await reply(m, stats_text())


@app.on_message(filters.private & filters.command("workers") & admin_only)
async def cmd_workers(_, m: Message):
    await reply(m, await workers_text(live=True), admin_workers_keyboard(pool.urls))


@app.on_message(filters.private & filters.command(["grant", "credits", "revoke", "ban", "unban", "user"]) & admin_only)
async def cmd_admin_user(_, m: Message):
    """
    /grant <uid> [days]        unlimited plan
    /credits <uid> <millions>  add (negative = deduct)
    /revoke <uid>              remove unlimited
    /ban <uid> | /unban <uid>
    /user <uid>                user card
    """
    cmd, args = m.command[0], m.command[1:]
    if not args or not args[0].lstrip("-").isdigit():
        return await reply(m, f"Usage: <code>/{cmd} &lt;user_id&gt; [value]</code>")
    uid = int(args[0])
    val = args[1] if len(args) > 1 else None

    if cmd == "grant":
        days = int(val) if val and val.isdigit() else settings.unlimited_days
        until = store.grant_unlimited(uid, days)
        await safe_send(uid, f"⭐ Aapka Unlimited plan activate ho gaya (till {until}).")
        return await reply(m, f"✅ Unlimited granted to <code>{uid}</code> till {until}")
    if cmd == "credits":
        try:
            chars = int(float(val) * MILLION) if val else 0
        except ValueError:
            chars = 0
        if not chars:
            return await reply(m, "Usage: <code>/credits &lt;user_id&gt; &lt;millions&gt;</code>")
        bal = store.add_credits(uid, chars)
        if chars > 0:
            await safe_send(uid, f"💎 {fmt_chars(chars)} characters add hue. Balance: {fmt_chars(bal)}")
        return await reply(m, f"✅ <code>{uid}</code> credits {'+' if chars > 0 else ''}{fmt_chars(chars)} → {fmt_chars(bal)}")
    if cmd == "revoke":
        store.revoke_unlimited(uid)
        return await reply(m, f"✅ Unlimited revoked for <code>{uid}</code>")
    if cmd in ("ban", "unban"):
        store.set_banned(uid, cmd == "ban")
        return await reply(m, f"✅ <code>{uid}</code> {'banned' if cmd == 'ban' else 'unbanned'}")

    u = store.get_user(uid)
    if not u:
        return await reply(m, "User not found.")
    a = avail(uid)
    await reply(m, (f"👤 <code>{uid}</code> @{esc(u.get('username') or '-')} {esc(u.get('first_name'))}\n"
                    f"lang={u['lang']} banned={u['banned']} unlimited_until={u['unlimited_until']}\n"
                    f"credits={fmt_chars(u['credits'])} free_left={fmt_chars(a['free_left'])}\n"
                    f"files={u['total_files']} chars={fmt_chars(u['total_chars'])}\nseen={u['last_seen']}"))


def stats_text() -> str:
    uc, js, rv = store.user_count(), store.job_stats(), store.revenue()
    return "\n".join([
        "📊 <b>Stats</b>",
        f"👥 Users {uc['total']} • active 7d {uc['active7d']} • unlimited {uc['unlimited']}",
        f"💎 Credits outstanding {fmt_chars(uc['credits_outstanding'])}",
        f"📚 Jobs {js['total']} (✅{js['done']} ❌{js['failed']}) • {fmt_chars(js['chars'])} chars",
        f"📅 Today {js['today']} jobs • {fmt_chars(js['today_chars'])} chars",
        f"💰 Revenue ₹{rv['total_inr']} ({rv['count']} payments) • this month ₹{rv['month_inr']}",
        f"⚙️ Queue {jobs.queue.qsize()} • running {len(jobs.running)} • workers {len(pool.healthy())}/{len(pool)}",
    ])


async def workers_text(live: bool = False) -> str:
    if not pool.urls:
        return "🖥 <b>Workers</b>\n\nKoi worker nahi. ➕ Add se shuru karein."
    if live:
        await pool.check_all()
    healthy = set(pool.healthy())
    lines = [f"🖥 <b>Workers</b>  ({len(healthy)}/{len(pool)} healthy)\n"]
    for i, url in enumerate(pool.urls):
        st = pool.stats.get(url, {"ok": 0, "fail": 0})
        info = pool.last_health.get(url, (True, ""))[1]
        lines.append(f"{i + 1}. {'🟢' if url in healthy else '🔴'} <code>{esc(url.replace('https://', ''))}</code>\n"
                     f"    ok {st['ok']} • fail {st['fail']}{(' • ' + esc(info)) if info else ''}")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Documents
# --------------------------------------------------------------------------- #
@app.on_message(filters.private & filters.document)
async def on_document(_, m: Message):
    u = touch(m)
    if not u:
        return
    uid = m.from_user.id
    if banned(uid):
        return await reply(m, "🚫 Aapka access band hai. Support se contact karein.")
    doc = m.document
    name = doc.file_name or "book.epub"
    if not name.lower().endswith(".epub"):
        return await reply(m, "⚠️ Sirf <b>.epub</b> files supported hain.")
    if doc.file_size and doc.file_size > settings.max_file_mb * 1024 * 1024:
        return await reply(m, f"⚠️ File {settings.max_file_mb} MB se badi hai.")
    if not pool.urls:
        return await reply(m, "⚠️ Abhi koi translation worker online nahi hai. Thodi der baad try karein.")
    if jobs.has_active(uid):
        return await reply(m, "⏳ Aapki ek file already process ho rahi hai. /cancel se rok sakte hain.")
    old = jobs.user_pending(uid)
    if old:
        jobs.drop_pending(old.token)
        await safe_edit(old.chat_id, old.msg_id, "✖️ Replaced by a new file.")

    status = await reply(m, "📥 Downloading & analysing…")
    if not status:
        return
    path = os.path.join(settings.download_dir, f"{uid}_{int(time.time())}.epub")
    try:
        got = await m.download(file_name=path)
        if not got:
            raise RuntimeError("download returned nothing")
    except Exception as exc:  # noqa: BLE001
        log.warning("download failed: %s", exc)
        cleanup(path)
        return await safe_edit(m.chat.id, status.id, "❌ Download failed. Dobara bhejein.")

    try:
        p = await jobs.prepare(uid, m.chat.id, path, name, u["lang"], status.id)
    except EpubError as exc:
        cleanup(path)
        return await safe_edit(m.chat.id, status.id, f"❌ {esc(exc)}")
    except Exception as exc:  # noqa: BLE001
        log.exception("analyse failed")
        cleanup(path)
        return await safe_edit(m.chat.id, status.id, f"❌ File analyse nahi ho payi ({type(exc).__name__}).")
    await show_confirm(p.token)


async def show_confirm(token: str) -> None:
    p = jobs.pending.get(token)
    if not p:
        return
    a = avail(p.user_id)
    chars = p.analysis.total_chars
    enough = a["unlimited"] or a["total"] >= chars
    short_m = 0 if enough else max(1, -(-(chars - a["total"]) // MILLION))
    await safe_edit(p.chat_id, p.msg_id,
                    confirm_text(p.analysis.title, p.file_name, chars, p.analysis.total_nodes, p.lang, a),
                    confirm_keyboard(token, enough, short_m))


# --------------------------------------------------------------------------- #
# Text: reply-keyboard buttons + conversational states
# --------------------------------------------------------------------------- #
@app.on_message(filters.private & filters.text & ~filters.command(USER_CMDS + ADMIN_CMDS))
async def on_text(_, m: Message):
    u = touch(m)
    if not u:
        return
    uid = m.from_user.id
    text = (m.text or "").strip()
    st = state.get(uid)
    if st:
        handled = await handle_state(m, uid, st, text)
        if handled:
            return

    if text == BTN_PLAN:
        return await reply(m, plan_text(avail(uid), u), plan_keyboard())
    if text == BTN_LANG:
        return await reply(m, "🌐 Target language chunein:", lang_keyboard(u["lang"]))
    if text == BTN_BUY:
        return await reply(m, buy_menu_text(avail(uid)), buy_menu_keyboard())
    if text == BTN_ADMIN and is_admin(uid):
        return await reply(m, "⚙️ <b>Admin panel</b>", admin_menu_keyboard())
    await reply(m, "📎 Translate karne ke liye <b>.epub</b> file bhejein. Help: /help", main_keyboard(is_admin(uid)))


async def handle_state(m: Message, uid: int, st: dict, text: str) -> bool:
    mode = st.get("mode")

    if mode == "custom_credits":
        mm = parse_millions(text)
        if mm is None:
            await reply(m, f"⚠️ {settings.min_credit_millions}–{settings.max_credit_millions} ke beech number bhejein "
                           f"(e.g. <code>4</code> = 4M characters = ₹{credits_price(4)}).", cancel_keyboard("buy:menu"))
            return True
        state.pop(uid, None)
        await reply(m, method_text("credits", mm), pay_method_keyboard("credits", mm, rzp.enabled, bool(settings.upi_id)))
        return True

    if mode == "upi_utr":
        utr = text.replace(" ", "")
        if not (8 <= len(utr) <= 40) or not utr.isalnum():
            await reply(m, "⚠️ Valid UTR / transaction ID bhejein (usually 12 digit number).",
                        cancel_keyboard(f"pay:abort:{st['payment_id']}"))
            return True
        pay = store.get_payment(st["payment_id"])
        state.pop(uid, None)
        if not pay or pay["status"] not in ("created", "pending"):
            await reply(m, "Yeh request ab valid nahi hai. /buy se dobara shuru karein.")
            return True
        store.set_payment_status(pay["id"], "pending", ref=utr)
        desc, amt = plan_summary(pay["kind"], pay["millions"])
        await reply(m, "🕒 Shukriya! Payment verify hone ke baad plan turant activate ho jayega (usually kuch minute).")
        await notify_admins(f"💰 <b>UPI payment review</b> #{pay['id']}\nUser: <code>{uid}</code> "
                            f"@{esc(m.from_user.username or '-')}\nPlan: {desc} — ₹{amt}\nUTR: <code>{esc(utr)}</code>",
                            admin_review_keyboard(pay["id"]))
        return True

    if not is_admin(uid):
        state.pop(uid, None)
        return False

    if mode == "add_worker":
        state.pop(uid, None)
        url = pool.normalize(text)
        if url in pool.urls:
            await reply(m, "⚠️ Worker already added.", back_keyboard("adm:workers"))
            return True
        ok, info = await pool.check(url)
        if not ok:
            await reply(m, f"❌ Worker reachable nahi ({esc(info)}). URL check karein.", back_keyboard("adm:workers"))
            return True
        pool.add(url)
        await reply(m, f"✅ Worker added — {esc(info)}\nTotal: {len(pool)}", back_keyboard("adm:workers"))
        return True

    if mode == "broadcast":
        state.pop(uid, None)
        asyncio.create_task(broadcast(uid, text))
        await reply(m, "📣 Broadcast started…")
        return True

    state.pop(uid, None)
    return False


def method_text(kind: str, millions: int) -> str:
    desc, amt = plan_summary(kind, millions)
    return f"🧾 <b>{desc}</b> — <b>₹{amt}</b>\n\nPayment method chunein:"


async def broadcast(admin_id: int, text: str) -> None:
    sent = failed = 0
    for uid in store.all_user_ids():
        if await safe_send(uid, text):
            sent += 1
        else:
            failed += 1
        await asyncio.sleep(0.05)
    await safe_send(admin_id, f"📣 Broadcast done: sent {sent}, failed {failed}")


# --------------------------------------------------------------------------- #
# Callback queries
# --------------------------------------------------------------------------- #
@app.on_callback_query()
async def on_callback(_, cq: CallbackQuery):
    u = touch(cq)
    if not u:
        return await answer(cq)
    uid = cq.from_user.id
    data = cq.data or ""
    parts = data.split(":")
    head = parts[0]
    try:
        if head == "noop":
            return await answer(cq)
        if head == "cancel":
            state.pop(uid, None)
            await answer(cq, "Cancelled")
            return await edit_cq(cq, "✖️ Cancelled.")
        if head == "lang" and len(parts) >= 2:
            return await cb_lang(cq, uid, parts)
        if head == "langpg" and len(parts) >= 2 and parts[1].isdigit():
            back = ":".join(parts[2:])
            back = None if back in ("", "-") else back
            return await edit_cq(cq, "🌐 Target language chunein:", lang_keyboard(u["lang"], int(parts[1]), back))
        if head == "job" and len(parts) >= 3:
            return await cb_job(cq, uid, parts)
        if head == "me" and len(parts) >= 2:
            return await cb_me(cq, uid, parts)
        if head == "buy" and len(parts) >= 2:
            return await cb_buy(cq, uid, parts)
        if head == "pay" and len(parts) >= 2:
            return await cb_pay(cq, uid, parts)
        if head == "adm" and len(parts) >= 2:
            if not is_admin(uid):
                return await answer(cq, "Admins only", True)
            return await cb_admin(cq, uid, parts)
        await answer(cq)
    except Exception:  # noqa: BLE001
        log.exception("callback failed: %s", data)
        await answer(cq, "Kuch galat ho gaya, dobara try karein.", True)


async def cb_lang(cq: CallbackQuery, uid: int, parts: list) -> None:
    code = parts[1]
    if code not in LANGUAGES:
        return await answer(cq, "Unknown language", True)
    store.set_lang(uid, code)
    back = ":".join(parts[3:])
    back = None if back in ("", "-") else back
    await answer(cq, f"✅ {lang_name(code)}")
    if back and back.startswith("job:view:"):
        token = back.split(":")[2]
        p = jobs.pending.get(token)
        if p and p.user_id == uid:
            p.lang = code
            return await show_confirm(token)
    await edit_cq(cq, f"🌐 Language: <b>{lang_name(code)}</b>\nAb <b>.epub</b> file bhejein.")


async def cb_job(cq: CallbackQuery, uid: int, parts: list) -> None:
    action, ref = parts[1], parts[2]
    if action == "stop":
        jid = int(ref) if ref.isdigit() else -1
        job = jobs.running.get(jid) or jobs.queued_job(jid)
        if not job or (job.user_id != uid and not is_admin(uid)):
            return await answer(cq, "Job nahi mila", True)
        jobs.cancel_job(job.id)
        return await answer(cq, "⏹ Cancelling…")

    p = jobs.pending.get(ref)
    if not p or p.user_id != uid:
        await answer(cq, "Yeh request expire ho gayi. File dobara bhejein.", True)
        return await edit_cq(cq, "⌛ Expired — file dobara bhejein.")
    if action == "cancel":
        jobs.drop_pending(ref)
        await answer(cq, "Cancelled")
        return await edit_cq(cq, "✖️ Cancelled.")
    if action == "lang":
        await answer(cq)
        return await edit_cq(cq, "🌐 Is file ke liye language chunein:", lang_keyboard(p.lang, 0, f"job:view:{ref}"))
    if action == "view":
        await answer(cq)
        return await show_confirm(ref)
    if action == "start":
        if jobs.has_active(uid):
            return await answer(cq, "Ek job already chal raha hai.", True)
        if not pool.urls:
            return await answer(cq, "Koi worker online nahi hai.", True)
        try:
            breakdown = store.consume(uid, p.analysis.total_chars, settings.free_daily_chars)
        except ValueError:
            await answer(cq, "Balance kam hai.", True)
            return await show_confirm(ref)
        job = await jobs.enqueue(p, breakdown)
        pos = jobs.position(job)
        await answer(cq, "✅ Queued")
        wait = f"Queue position: {pos}" if pos > 0 and len(jobs.running) >= settings.max_concurrent_jobs else "Starting…"
        return await edit_cq(cq, f"🕒 <b>{esc(job.title)}</b> → {lang_name(job.lang)}\n{wait}",
                             progress_keyboard(job.id))
    await answer(cq)


async def cb_me(cq: CallbackQuery, uid: int, parts: list) -> None:
    if parts[1] == "history":
        rows = store.recent_jobs(uid, 8)
        if not rows:
            return await answer(cq, "Abhi koi history nahi.", True)
        icon = {"done": "✅", "failed": "❌", "cancelled": "✖️", "running": "⏳", "queued": "🕒"}
        lines = [f"{icon.get(r['status'], '•')} {esc(r['file_name'] or 'book')} → {lang_name(r['lang'] or '')} "
                 f"• {fmt_chars(r['chars'])}" for r in rows]
        await answer(cq)
        return await edit_cq(cq, "📜 <b>Recent files</b>\n" + "\n".join(lines), plan_keyboard())
    await answer(cq)


async def cb_buy(cq: CallbackQuery, uid: int, parts: list) -> None:
    what = parts[1]
    if what == "menu":
        state.pop(uid, None)
        await answer(cq)
        return await edit_cq(cq, buy_menu_text(avail(uid)), buy_menu_keyboard())
    if what == "custom":
        state[uid] = {"mode": "custom_credits"}
        await answer(cq)
        return await edit_cq(cq,
                             f"✏️ Kitne <b>million characters</b> chahiye? Number bhejein "
                             f"({settings.min_credit_millions}–{settings.max_credit_millions}).\n"
                             f"₹{settings.credit_price_per_million_inr} per 1M — e.g. <code>6</code> = 6M = ₹{credits_price(6)}",
                             cancel_keyboard("buy:menu"))
    if what == "unlimited":
        await answer(cq, "Unlimited already active — aage badhne se extend hoga." if store.is_unlimited(uid) else "")
        return await edit_cq(cq, method_text("unlimited", 0),
                             pay_method_keyboard("unlimited", 0, rzp.enabled, bool(settings.upi_id)))
    if what == "credits":
        mm = parse_millions(parts[2]) if len(parts) > 2 else None
        if mm is None:
            return await answer(cq, "Invalid amount", True)
        await answer(cq)
        return await edit_cq(cq, method_text("credits", mm),
                             pay_method_keyboard("credits", mm, rzp.enabled, bool(settings.upi_id)))
    await answer(cq)


async def cb_pay(cq: CallbackQuery, uid: int, parts: list) -> None:
    what = parts[1]
    if what == "contact":
        msg = f"Payment ke liye contact karein: {settings.support_contact}" if settings.support_contact \
            else "Payment gateway abhi set nahi hai. Admin se contact karein."
        return await answer(cq, msg, True)

    if what in ("rzp", "upi") and len(parts) >= 4:
        kind, mm_raw = parts[2], parts[3]
        mm = int(mm_raw) if mm_raw.isdigit() else -1
        if kind not in ("unlimited", "credits") or (kind == "credits" and parse_millions(str(mm)) is None):
            return await answer(cq, "Invalid plan", True)
        desc, amt = plan_summary(kind, mm)
        for old in store.pending_payments(uid):              # one open payment per user
            store.set_payment_status(old["id"], "expired", note="replaced")

        if what == "rzp":
            if not rzp.enabled:
                return await answer(cq, "Online payment unavailable", True)
            pid = store.create_payment(uid, kind, mm, amt, "razorpay")
            try:
                link_id, url = await rzp.create_link(uid, kind, mm, amt, pid)
            except Exception as exc:  # noqa: BLE001
                log.warning("razorpay link failed: %s", exc)
                store.set_payment_status(pid, "expired", note="link_failed")
                return await answer(cq, "Payment link nahi ban paya, thodi der baad try karein.", True)
            store.set_payment_status(pid, "pending", ref=link_id)
            await answer(cq)
            return await edit_cq(cq, f"🧾 <b>{desc}</b> — ₹{amt}\n\nNiche button se payment karein, phir "
                                     "<b>Verify payment</b> dabayein. Plan turant activate ho jayega.",
                                 rzp_keyboard(url, pid))

        if not settings.upi_id:
            return await answer(cq, "UPI unavailable", True)
        pid = store.create_payment(uid, kind, mm, amt, "upi")
        note = f"EPUB{pid}"
        state[uid] = {"mode": "upi_utr", "payment_id": pid}
        await answer(cq)
        return await edit_cq(cq,
                             f"🧾 <b>{desc}</b> — <b>₹{amt}</b>\n\n"
                             f"UPI ID: <code>{esc(settings.upi_id)}</code>\nName: {esc(settings.upi_name)}\n"
                             f"Amount: <code>{amt}</code> • Note: <code>{note}</code>\n\n"
                             "Payment ke baad <b>UTR / Transaction ID</b> yahan bhejein.",
                             upi_keyboard(pid))

    pid = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    pay = store.get_payment(pid)
    if not pay or pay["user_id"] != uid:
        return await answer(cq, "Payment nahi mila", True)
    if what == "abort":
        state.pop(uid, None)
        if pay["status"] in ("created", "pending"):
            store.set_payment_status(pid, "expired", note="user_abort")
        await answer(cq, "Cancelled")
        return await edit_cq(cq, "✖️ Payment cancelled.", back_keyboard("buy:menu"))
    if what == "check":
        if pay["status"] == "paid":
            return await answer(cq, "Already activated ✅", True)
        if pay["method"] != "razorpay" or not pay["ref"]:
            return await answer(cq, "Manual verification pending.", True)
        paid = await rzp.is_paid(pay["ref"])
        if paid is None:
            return await answer(cq, "Verify nahi ho paya, 1 min baad try karein.", True)
        if not paid:
            return await answer(cq, "Payment abhi receive nahi hua. Pay karke dobara verify karein.", True)
        msg = fulfil(store, pay, note="auto_verified")
        await answer(cq, "✅ Activated!")
        await edit_cq(cq, f"✅ <b>Payment received!</b>\n{msg}", plan_keyboard())
        return await notify_admins(f"💰 Razorpay paid #{pid} • <code>{uid}</code> • ₹{pay['amount_inr']} • "
                                   f"{pay['kind']} {pay['millions']}M")
    await answer(cq)


# ---- admin callbacks -------------------------------------------------------- #
async def cb_admin(cq: CallbackQuery, uid: int, parts: list) -> None:
    sect = parts[1]
    if sect == "menu":
        state.pop(uid, None)
        await answer(cq)
        return await edit_cq(cq, "⚙️ <b>Admin panel</b>", admin_menu_keyboard())
    if sect == "stats":
        await answer(cq)
        return await edit_cq(cq, stats_text(), back_keyboard("adm:menu"))
    if sect == "workers":
        await answer(cq)
        return await edit_cq(cq, await workers_text(), admin_workers_keyboard(pool.urls))
    if sect == "w" and len(parts) >= 3:
        act = parts[2]
        if act == "add":
            state[uid] = {"mode": "add_worker"}
            await answer(cq)
            return await edit_cq(cq, "➕ Worker URL bhejein (e.g. <code>https://xyz.onrender.com</code>):",
                                 cancel_keyboard("adm:workers"))
        if act == "del":
            await answer(cq)
            return await edit_cq(cq, "➖ Kaunsa worker hatana hai?", admin_del_keyboard(pool.urls))
        if act == "rm" and len(parts) >= 4 and parts[3].isdigit():
            i = int(parts[3])
            if 0 <= i < len(pool.urls):
                pool.remove(pool.urls[i])
                await answer(cq, "Removed")
            return await edit_cq(cq, await workers_text(), admin_workers_keyboard(pool.urls))
        if act == "ping":
            await answer(cq, "Pinging…")
            return await edit_cq(cq, await workers_text(live=True), admin_workers_keyboard(pool.urls))
    if sect == "payments":
        rows = [r for r in store.pending_payments(method="upi") if r["ref"]]
        await answer(cq)
        if not rows:
            return await edit_cq(cq, "💰 Koi pending UPI payment nahi.", back_keyboard("adm:menu"))
        r = rows[0]
        desc, amt = plan_summary(r["kind"], r["millions"])
        return await edit_cq(cq, f"💰 <b>Pending #{r['id']}</b> ({len(rows)} total)\nUser <code>{r['user_id']}</code>\n"
                                 f"{desc} — ₹{amt}\nUTR <code>{esc(r['ref'])}</code>\n{r['created_at']}",
                             admin_review_keyboard(r["id"]))
    if sect == "pay" and len(parts) >= 4 and parts[3].isdigit():
        decision, pid = parts[2], int(parts[3])
        pay = store.get_payment(pid)
        if not pay:
            return await answer(cq, "Not found", True)
        if pay["status"] != "pending":
            return await answer(cq, f"Already {pay['status']}", True)
        if decision == "ok":
            msg = fulfil(store, pay, note=f"approved_by_{uid}")
            await safe_send(pay["user_id"], f"✅ <b>Payment verified!</b>\n{msg}", plan_keyboard())
            await answer(cq, "Approved ✅")
            return await edit_cq(cq, f"✅ Approved #{pid} — {msg}", back_keyboard("adm:payments"))
        store.set_payment_status(pid, "rejected", note=f"rejected_by_{uid}")
        await safe_send(pay["user_id"], f"❌ Payment #{pid} verify nahi ho paya. Agar aapne pay kiya hai to "
                                        "sahi UTR ke saath /buy se dobara request karein.")
        await answer(cq, "Rejected")
        return await edit_cq(cq, f"❌ Rejected #{pid}", back_keyboard("adm:payments"))
    if sect == "bcast":
        state[uid] = {"mode": "broadcast"}
        await answer(cq)
        return await edit_cq(cq, "📣 Broadcast message bhejein (HTML ok):", cancel_keyboard("adm:menu"))
    if sect == "maint":
        await answer(cq)
        return await edit_cq(cq, "🧹 <b>Maintenance</b>", admin_maint_keyboard())
    if sect == "m" and len(parts) >= 3:
        act = parts[2]
        if act == "cooldown":
            pool.fail_at.clear()
            await answer(cq, "Cooldowns cleared")
        elif act == "killjobs":
            await answer(cq, f"{jobs.cancel_all()} jobs cancelled")
        elif act == "expire":
            await answer(cq, f"{store.expire_old_pending(hours=24)} expired")
        return await edit_cq(cq, "🧹 <b>Maintenance</b>", admin_maint_keyboard())
    await answer(cq)


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
async def main() -> None:
    for f in os.listdir(settings.download_dir):            # leftovers from a previous crash
        cleanup(os.path.join(settings.download_dir, f))

    await app.start()
    me = await app.get_me()
    jobs.start()
    keepalive = asyncio.create_task(pool.keepalive_loop(), name="keepalive")
    log.info("Bot @%s started • workers=%d • users=%d", me.username, len(pool), store.user_count()["total"])
    await notify_admins(f"🟢 Bot restarted • workers {len(pool)}")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    await stop.wait()

    log.info("Shutting down…")
    keepalive.cancel()
    await jobs.stop()
    await pool.close()
    await app.stop()
    store.close()


if __name__ == "__main__":
    try:
        app.run(main())
    except KeyboardInterrupt:
        pass
