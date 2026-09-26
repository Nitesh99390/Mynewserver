"""
UI layer – texts and keyboards.

Design rule: every screen shows only the buttons that are useful *right now*
(2–6 buttons max).  Deeper options live one tap away behind a menu button.
All texts use HTML parse mode; anything user-supplied goes through esc().
"""

from __future__ import annotations

import html
from typing import List, Optional

from pyrogram.types import InlineKeyboardButton as B
from pyrogram.types import InlineKeyboardMarkup as KB
from pyrogram.types import KeyboardButton, ReplyKeyboardMarkup

from .config import LANGUAGES, lang_name, settings
from .payments import credits_price, fmt_chars

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
