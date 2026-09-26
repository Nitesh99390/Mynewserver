"""
Central configuration (environment / .env driven).

All tunables live here so bot.py and the core modules never read os.environ
directly.  Every value has a safe default so the bot boots with only the four
mandatory Telegram variables set.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Set

from dotenv import load_dotenv

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
