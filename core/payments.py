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

from __future__ import annotations

import logging
import re
from typing import Optional, Tuple
from urllib.parse import quote

import aiohttp

from .config import MILLION, settings
from .store import Store

log = logging.getLogger("payments")


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
