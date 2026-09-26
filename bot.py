"""
EPUB Translator – Master Bot  v3
================================
Runs on your own server (Oracle VPS etc.).  Receives EPUB files on Telegram,
splits the text into batches, fans them out to the translation workers
(app.py) hosted on Render / Vercel free tiers, rebuilds the EPUB and sends it
back.

    pip install -r requirements-bot.txt
    cp .env.example .env        # fill API_ID, API_HASH, BOT_TOKEN, OWNER_ID
    python bot.py

See README.md for the full deployment guide (systemd unit included).
"""

from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import signal
import time
from typing import Dict, Optional

from pyrogram import Client, enums, filters
from pyrogram.errors import FloodWait, MessageNotModified, RPCError
from pyrogram.types import CallbackQuery, Message

from core import ui
from core.config import LANGUAGES, MILLION, lang_name, settings
from core.epub_engine import EpubError, cleanup
from core.jobs import Job, JobManager, Pending
from core.payments import Razorpay, credits_price, fmt_chars, fulfil, parse_millions, plan_summary
from core.store import Store
from core.workers import WorkerPool

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
log = logging.getLogger("bot")

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
                    ui.progress_text(job.title, job.lang, 0, 0, job.progress.total, 0, None,
                                     len(pool.healthy()), len(pool)),
                    ui.progress_keyboard(job.id))


async def job_progress(job: Job) -> None:
    now = time.time()
    if now - _last_edit.get(job.id, 0) < 4:      # Telegram edit rate-limit friendly
        return
    _last_edit[job.id] = now
    p = job.progress
    await safe_edit(job.chat_id, job.msg_id,
                    ui.progress_text(job.title, job.lang, p.pct, p.done, p.total, p.elapsed, p.eta,
                                     len(pool.healthy()), len(pool)),
                    ui.progress_keyboard(job.id))


async def job_finished(job: Job, out_path: Optional[str], error: Optional[str]) -> None:
    _last_edit.pop(job.id, None)
    if error == "cancelled":
        return await safe_edit(job.chat_id, job.msg_id, "✖️ Translation cancelled. Characters refund ho gaye.")
    if error or not out_path:
        return await safe_edit(job.chat_id, job.msg_id,
                               f"❌ <b>Translation failed</b>\n{ui.esc(error or 'Unknown error')}\n\n"
                               "Characters refund ho gaye. Thodi der baad dobara try karein.")

    await safe_edit(job.chat_id, job.msg_id, "✅ Translation complete — uploading…")
    p = job.progress
    warn = f"\n⚠️ {p.failed_segments} segments untranslated (worker errors)" if p.failed_segments else ""
    base = os.path.splitext(job.file_name)[0]
    caption = (f"📚 <b>{ui.esc(job.title)}</b>\n🌐 {lang_name(job.lang)} • {fmt_chars(job.analysis.total_chars)} chars"
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
    text = ui.welcome_text(m.from_user.first_name or "friend") if m.command[0] == "start" else ui.help_text()
    await reply(m, text, ui.main_keyboard(is_admin(uid)))


@app.on_message(filters.private & filters.command("plan"))
async def cmd_plan(_, m: Message):
    u = touch(m)
    if u:
        await reply(m, ui.plan_text(avail(m.from_user.id), u), ui.plan_keyboard())


@app.on_message(filters.private & filters.command("lang"))
async def cmd_lang(_, m: Message):
    u = touch(m)
    if u:
        await reply(m, "🌐 Target language chunein:", ui.lang_keyboard(u["lang"]))


@app.on_message(filters.private & filters.command(["buy", "pay"]))
async def cmd_buy(_, m: Message):
    if touch(m):
        await reply(m, ui.buy_menu_text(avail(m.from_user.id)), ui.buy_menu_keyboard())


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
    await reply(m, "⚙️ <b>Admin panel</b>", ui.admin_menu_keyboard())


@app.on_message(filters.private & filters.command("stats") & admin_only)
async def cmd_stats(_, m: Message):
    await reply(m, stats_text())


@app.on_message(filters.private & filters.command("workers") & admin_only)
async def cmd_workers(_, m: Message):
    await reply(m, await workers_text(live=True), ui.admin_workers_keyboard(pool.urls))


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
    await reply(m, (f"👤 <code>{uid}</code> @{ui.esc(u.get('username') or '-')} {ui.esc(u.get('first_name'))}\n"
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
        lines.append(f"{i + 1}. {'🟢' if url in healthy else '🔴'} <code>{ui.esc(url.replace('https://', ''))}</code>\n"
                     f"    ok {st['ok']} • fail {st['fail']}{(' • ' + ui.esc(info)) if info else ''}")
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
        return await safe_edit(m.chat.id, status.id, f"❌ {ui.esc(exc)}")
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
                    ui.confirm_text(p.analysis.title, p.file_name, chars, p.analysis.total_nodes, p.lang, a),
                    ui.confirm_keyboard(token, enough, short_m))


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

    if text == ui.BTN_PLAN:
        return await reply(m, ui.plan_text(avail(uid), u), ui.plan_keyboard())
    if text == ui.BTN_LANG:
        return await reply(m, "🌐 Target language chunein:", ui.lang_keyboard(u["lang"]))
    if text == ui.BTN_BUY:
        return await reply(m, ui.buy_menu_text(avail(uid)), ui.buy_menu_keyboard())
    if text == ui.BTN_ADMIN and is_admin(uid):
        return await reply(m, "⚙️ <b>Admin panel</b>", ui.admin_menu_keyboard())
    await reply(m, "📎 Translate karne ke liye <b>.epub</b> file bhejein. Help: /help", ui.main_keyboard(is_admin(uid)))


async def handle_state(m: Message, uid: int, st: dict, text: str) -> bool:
    mode = st.get("mode")

    if mode == "custom_credits":
        mm = parse_millions(text)
        if mm is None:
            await reply(m, f"⚠️ {settings.min_credit_millions}–{settings.max_credit_millions} ke beech number bhejein "
                           f"(e.g. <code>4</code> = 4M characters = ₹{credits_price(4)}).", ui.cancel_keyboard("buy:menu"))
            return True
        state.pop(uid, None)
        await reply(m, method_text("credits", mm), ui.pay_method_keyboard("credits", mm, rzp.enabled, bool(settings.upi_id)))
        return True

    if mode == "upi_utr":
        utr = text.replace(" ", "")
        if not (8 <= len(utr) <= 40) or not utr.isalnum():
            await reply(m, "⚠️ Valid UTR / transaction ID bhejein (usually 12 digit number).",
                        ui.cancel_keyboard(f"pay:abort:{st['payment_id']}"))
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
                            f"@{ui.esc(m.from_user.username or '-')}\nPlan: {desc} — ₹{amt}\nUTR: <code>{ui.esc(utr)}</code>",
                            ui.admin_review_keyboard(pay["id"]))
        return True

    if not is_admin(uid):
        state.pop(uid, None)
        return False

    if mode == "add_worker":
        state.pop(uid, None)
        url = pool.normalize(text)
        if url in pool.urls:
            await reply(m, "⚠️ Worker already added.", ui.back_keyboard("adm:workers"))
            return True
        ok, info = await pool.check(url)
        if not ok:
            await reply(m, f"❌ Worker reachable nahi ({ui.esc(info)}). URL check karein.", ui.back_keyboard("adm:workers"))
            return True
        pool.add(url)
        await reply(m, f"✅ Worker added — {ui.esc(info)}\nTotal: {len(pool)}", ui.back_keyboard("adm:workers"))
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
            return await edit_cq(cq, "🌐 Target language chunein:", ui.lang_keyboard(u["lang"], int(parts[1]), back))
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
        return await edit_cq(cq, "🌐 Is file ke liye language chunein:", ui.lang_keyboard(p.lang, 0, f"job:view:{ref}"))
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
        return await edit_cq(cq, f"🕒 <b>{ui.esc(job.title)}</b> → {lang_name(job.lang)}\n{wait}",
                             ui.progress_keyboard(job.id))
    await answer(cq)


async def cb_me(cq: CallbackQuery, uid: int, parts: list) -> None:
    if parts[1] == "history":
        rows = store.recent_jobs(uid, 8)
        if not rows:
            return await answer(cq, "Abhi koi history nahi.", True)
        icon = {"done": "✅", "failed": "❌", "cancelled": "✖️", "running": "⏳", "queued": "🕒"}
        lines = [f"{icon.get(r['status'], '•')} {ui.esc(r['file_name'] or 'book')} → {lang_name(r['lang'] or '')} "
                 f"• {fmt_chars(r['chars'])}" for r in rows]
        await answer(cq)
        return await edit_cq(cq, "📜 <b>Recent files</b>\n" + "\n".join(lines), ui.plan_keyboard())
    await answer(cq)


async def cb_buy(cq: CallbackQuery, uid: int, parts: list) -> None:
    what = parts[1]
    if what == "menu":
        state.pop(uid, None)
        await answer(cq)
        return await edit_cq(cq, ui.buy_menu_text(avail(uid)), ui.buy_menu_keyboard())
    if what == "custom":
        state[uid] = {"mode": "custom_credits"}
        await answer(cq)
        return await edit_cq(cq,
                             f"✏️ Kitne <b>million characters</b> chahiye? Number bhejein "
                             f"({settings.min_credit_millions}–{settings.max_credit_millions}).\n"
                             f"₹{settings.credit_price_per_million_inr} per 1M — e.g. <code>6</code> = 6M = ₹{credits_price(6)}",
                             ui.cancel_keyboard("buy:menu"))
    if what == "unlimited":
        await answer(cq, "Unlimited already active — aage badhne se extend hoga." if store.is_unlimited(uid) else "")
        return await edit_cq(cq, method_text("unlimited", 0),
                             ui.pay_method_keyboard("unlimited", 0, rzp.enabled, bool(settings.upi_id)))
    if what == "credits":
        mm = parse_millions(parts[2]) if len(parts) > 2 else None
        if mm is None:
            return await answer(cq, "Invalid amount", True)
        await answer(cq)
        return await edit_cq(cq, method_text("credits", mm),
                             ui.pay_method_keyboard("credits", mm, rzp.enabled, bool(settings.upi_id)))
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
                                 ui.rzp_keyboard(url, pid))

        if not settings.upi_id:
            return await answer(cq, "UPI unavailable", True)
        pid = store.create_payment(uid, kind, mm, amt, "upi")
        note = f"EPUB{pid}"
        state[uid] = {"mode": "upi_utr", "payment_id": pid}
        await answer(cq)
        return await edit_cq(cq,
                             f"🧾 <b>{desc}</b> — <b>₹{amt}</b>\n\n"
                             f"UPI ID: <code>{ui.esc(settings.upi_id)}</code>\nName: {ui.esc(settings.upi_name)}\n"
                             f"Amount: <code>{amt}</code> • Note: <code>{note}</code>\n\n"
                             "Payment ke baad <b>UTR / Transaction ID</b> yahan bhejein.",
                             ui.upi_keyboard(pid))

    pid = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    pay = store.get_payment(pid)
    if not pay or pay["user_id"] != uid:
        return await answer(cq, "Payment nahi mila", True)
    if what == "abort":
        state.pop(uid, None)
        if pay["status"] in ("created", "pending"):
            store.set_payment_status(pid, "expired", note="user_abort")
        await answer(cq, "Cancelled")
        return await edit_cq(cq, "✖️ Payment cancelled.", ui.back_keyboard("buy:menu"))
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
        await edit_cq(cq, f"✅ <b>Payment received!</b>\n{msg}", ui.plan_keyboard())
        return await notify_admins(f"💰 Razorpay paid #{pid} • <code>{uid}</code> • ₹{pay['amount_inr']} • "
                                   f"{pay['kind']} {pay['millions']}M")
    await answer(cq)


# ---- admin callbacks -------------------------------------------------------- #
async def cb_admin(cq: CallbackQuery, uid: int, parts: list) -> None:
    sect = parts[1]
    if sect == "menu":
        state.pop(uid, None)
        await answer(cq)
        return await edit_cq(cq, "⚙️ <b>Admin panel</b>", ui.admin_menu_keyboard())
    if sect == "stats":
        await answer(cq)
        return await edit_cq(cq, stats_text(), ui.back_keyboard("adm:menu"))
    if sect == "workers":
        await answer(cq)
        return await edit_cq(cq, await workers_text(), ui.admin_workers_keyboard(pool.urls))
    if sect == "w" and len(parts) >= 3:
        act = parts[2]
        if act == "add":
            state[uid] = {"mode": "add_worker"}
            await answer(cq)
            return await edit_cq(cq, "➕ Worker URL bhejein (e.g. <code>https://xyz.onrender.com</code>):",
                                 ui.cancel_keyboard("adm:workers"))
        if act == "del":
            await answer(cq)
            return await edit_cq(cq, "➖ Kaunsa worker hatana hai?", ui.admin_del_keyboard(pool.urls))
        if act == "rm" and len(parts) >= 4 and parts[3].isdigit():
            i = int(parts[3])
            if 0 <= i < len(pool.urls):
                pool.remove(pool.urls[i])
                await answer(cq, "Removed")
            return await edit_cq(cq, await workers_text(), ui.admin_workers_keyboard(pool.urls))
        if act == "ping":
            await answer(cq, "Pinging…")
            return await edit_cq(cq, await workers_text(live=True), ui.admin_workers_keyboard(pool.urls))
    if sect == "payments":
        rows = [r for r in store.pending_payments(method="upi") if r["ref"]]
        await answer(cq)
        if not rows:
            return await edit_cq(cq, "💰 Koi pending UPI payment nahi.", ui.back_keyboard("adm:menu"))
        r = rows[0]
        desc, amt = plan_summary(r["kind"], r["millions"])
        return await edit_cq(cq, f"💰 <b>Pending #{r['id']}</b> ({len(rows)} total)\nUser <code>{r['user_id']}</code>\n"
                                 f"{desc} — ₹{amt}\nUTR <code>{ui.esc(r['ref'])}</code>\n{r['created_at']}",
                             ui.admin_review_keyboard(r["id"]))
    if sect == "pay" and len(parts) >= 4 and parts[3].isdigit():
        decision, pid = parts[2], int(parts[3])
        pay = store.get_payment(pid)
        if not pay:
            return await answer(cq, "Not found", True)
        if pay["status"] != "pending":
            return await answer(cq, f"Already {pay['status']}", True)
        if decision == "ok":
            msg = fulfil(store, pay, note=f"approved_by_{uid}")
            await safe_send(pay["user_id"], f"✅ <b>Payment verified!</b>\n{msg}", ui.plan_keyboard())
            await answer(cq, "Approved ✅")
            return await edit_cq(cq, f"✅ Approved #{pid} — {msg}", ui.back_keyboard("adm:payments"))
        store.set_payment_status(pid, "rejected", note=f"rejected_by_{uid}")
        await safe_send(pay["user_id"], f"❌ Payment #{pid} verify nahi ho paya. Agar aapne pay kiya hai to "
                                        "sahi UTR ke saath /buy se dobara request karein.")
        await answer(cq, "Rejected")
        return await edit_cq(cq, f"❌ Rejected #{pid}", ui.back_keyboard("adm:payments"))
    if sect == "bcast":
        state[uid] = {"mode": "broadcast"}
        await answer(cq)
        return await edit_cq(cq, "📣 Broadcast message bhejein (HTML ok):", ui.cancel_keyboard("adm:menu"))
    if sect == "maint":
        await answer(cq)
        return await edit_cq(cq, "🧹 <b>Maintenance</b>", ui.admin_maint_keyboard())
    if sect == "m" and len(parts) >= 3:
        act = parts[2]
        if act == "cooldown":
            pool.fail_at.clear()
            await answer(cq, "Cooldowns cleared")
        elif act == "killjobs":
            await answer(cq, f"{jobs.cancel_all()} jobs cancelled")
        elif act == "expire":
            await answer(cq, f"{store.expire_old_pending(hours=24)} expired")
        return await edit_cq(cq, "🧹 <b>Maintenance</b>", ui.admin_maint_keyboard())
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
