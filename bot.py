"""
Master Bot  (Telegram EPUB Translator)
=======================================
Receives EPUB files on Telegram, splits the text into batches and sends them
to one or more Translation Workers (app.py) running on free hosts.
Translated EPUB is rebuilt *in place* so images, CSS, TOC and covers survive.

Environment (.env supported)
----------------------------
API_ID, API_HASH, BOT_TOKEN      Telegram credentials (required)
OWNER_ID                         Admin Telegram user id
WORKER_SECRET                    Optional, must match the workers' WORKER_SECRET
RAZORPAY_KEY_ID / _SECRET        Optional, enables /pay
FREE_DAILY_LIMIT                 Files per day for non-premium users (default 2)
DATA_FILE                        Persistence file (default data.json)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import date
from typing import Dict, List, Optional, Tuple

import aiohttp
import ebooklib
from bs4 import BeautifulSoup, NavigableString, Comment
from dotenv import load_dotenv
from ebooklib import epub
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("master")

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
WORKER_SECRET = os.environ.get("WORKER_SECRET", "").strip()
RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET")
FREE_DAILY_LIMIT = int(os.environ.get("FREE_DAILY_LIMIT", "2"))
DATA_FILE = os.environ.get("DATA_FILE", "data.json")
PREMIUM_PRICE_INR = int(os.environ.get("PREMIUM_PRICE_INR", "100"))

BATCH_SIZE = 40                 # texts per worker request
BATCH_CHARS = 4000              # chars per worker request
MAX_PARALLEL_BATCHES = 8        # concurrent requests across all workers
WORKER_TIMEOUT = aiohttp.ClientTimeout(total=60)
KEEPALIVE_INTERVAL = 600        # seconds
WORKER_FAIL_COOLDOWN = 120      # seconds a failing worker is skipped

# Tags whose text should never be translated
SKIP_TAGS = {"script", "style", "code", "pre", "kbd", "samp", "var", "math", "svg", "title"}
_NON_TEXT = re.compile(r"^[\W\d_]+$", re.UNICODE)   # numbers / punctuation only

LANGUAGES = {
    "hi": "Hindi", "bn": "Bengali", "ta": "Tamil", "te": "Telugu",
    "mr": "Marathi", "gu": "Gujarati", "ur": "Urdu", "kn": "Kannada",
    "ml": "Malayalam", "pa": "Punjabi", "en": "English",
}

# Optional Razorpay
rzp_client = None
if RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET:
    try:
        import razorpay  # type: ignore

        rzp_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
    except Exception as exc:  # pragma: no cover
        log.warning("Razorpay disabled: %s", exc)

app = Client("TranslatorBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
WORKERS: List[str] = []
users_db: Dict[int, dict] = {}
admin_state: Dict[int, str] = {}
worker_failures: Dict[str, float] = {}      # url -> timestamp of last failure
worker_stats: Dict[str, dict] = {}          # url -> {"ok": n, "fail": n}


def load_data() -> None:
    global WORKERS, users_db
    if not os.path.exists(DATA_FILE):
        return
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        WORKERS = [w.rstrip("/") for w in data.get("workers", [])]
        users_db = {int(k): v for k, v in data.get("users_db", {}).items()}
    except Exception as exc:
        log.error("Data load error: %s", exc)


def save_data() -> None:
    try:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"workers": WORKERS, "users_db": users_db}, f, ensure_ascii=False, indent=1)
        os.replace(tmp, DATA_FILE)
    except Exception as exc:
        log.error("Data save error: %s", exc)


load_data()


def get_user(user_id: int) -> dict:
    u = users_db.setdefault(user_id, {"lang": "hi", "has_subscription": False, "premium_until": None,
                                      "daily": {"date": "", "count": 0}})
    u.setdefault("daily", {"date": "", "count": 0})
    return u


def is_premium(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    u = get_user(user_id)
    until = u.get("premium_until")
    if until and until >= date.today().isoformat():
        return True
    return bool(u.get("has_subscription"))


def check_daily_quota(user_id: int) -> Tuple[bool, int]:
    """Return (allowed, remaining)."""
    if is_premium(user_id):
        return True, 9999
    u = get_user(user_id)
    today = date.today().isoformat()
    if u["daily"]["date"] != today:
        u["daily"] = {"date": today, "count": 0}
    remaining = FREE_DAILY_LIMIT - u["daily"]["count"]
    return remaining > 0, max(remaining, 0)


def consume_quota(user_id: int) -> None:
    if is_premium(user_id):
        return
    u = get_user(user_id)
    u["daily"]["count"] += 1
    save_data()


# --------------------------------------------------------------------------- #
# Queue
# --------------------------------------------------------------------------- #
translation_queue: asyncio.Queue = asyncio.Queue()
active_tasks: Dict[int, str] = {}


def get_main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton("🌐 Set Language"), KeyboardButton("💳 Premium (/pay)")],
        [KeyboardButton("📊 Queue Status"), KeyboardButton("❓ Help")],
    ]
    if user_id == OWNER_ID:
        buttons.append([KeyboardButton("➕ Add Worker"), KeyboardButton("➖ Del Worker")])
        buttons.append([KeyboardButton("🖥 Worker List"), KeyboardButton("🧹 Clear Stuck Tasks")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


# --------------------------------------------------------------------------- #
# Worker communication
# --------------------------------------------------------------------------- #
def _headers() -> dict:
    return {"X-Worker-Secret": WORKER_SECRET} if WORKER_SECRET else {}


def healthy_workers() -> List[str]:
    now = time.time()
    return [w for w in WORKERS if now - worker_failures.get(w, 0) > WORKER_FAIL_COOLDOWN]


def _mark(worker: str, ok: bool) -> None:
    s = worker_stats.setdefault(worker, {"ok": 0, "fail": 0})
    if ok:
        s["ok"] += 1
        worker_failures.pop(worker, None)
    else:
        s["fail"] += 1
        worker_failures[worker] = time.time()


async def keep_workers_alive() -> None:
    """Ping every worker so free-tier hosts don't go to sleep."""
    while True:
        if WORKERS:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                for worker in list(WORKERS):
                    try:
                        async with session.get(f"{worker}/") as r:
                            if r.status == 200:
                                worker_failures.pop(worker, None)
                    except Exception:
                        pass
        await asyncio.sleep(KEEPALIVE_INTERVAL)


async def check_worker(session: aiohttp.ClientSession, url: str) -> Tuple[bool, str]:
    try:
        async with session.get(f"{url}/health", headers=_headers()) as r:
            if r.status != 200:
                return False, f"HTTP {r.status}"
            data = await r.json(content_type=None)
            return True, f"v{data.get('version', '?')} up {data.get('uptime_s', 0)}s"
    except Exception as exc:
        return False, str(exc)[:60]


async def translate_batch_req(session: aiohttp.ClientSession, text_list: List[str],
                              target_lang: str, worker_hint: int) -> List[str]:
    """
    Send a batch to a worker; on failure, fall over to the other workers.
    Never raises – returns the original texts as a last resort.
    """
    candidates = healthy_workers() or list(WORKERS)
    if not candidates:
        raise RuntimeError("All workers are offline or deleted!")
    # rotate so different batches start at different workers
    start = worker_hint % len(candidates)
    order = candidates[start:] + candidates[:start]

    for worker in order:
        for attempt in range(2):
            try:
                async with session.post(
                    f"{worker}/translate",
                    json={"text_list": text_list, "lang": target_lang},
                    headers=_headers(),
                ) as resp:
                    if resp.status == 401:
                        log.error("Worker %s rejected secret", worker)
                        _mark(worker, False)
                        break
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}")
                    result = await resp.json(content_type=None)
                if result.get("success") and isinstance(result.get("translated"), list) \
                        and len(result["translated"]) == len(text_list):
                    _mark(worker, True)
                    return result["translated"]
                raise RuntimeError(result.get("error", "bad response"))
            except Exception as exc:
                log.warning("Worker %s failed (%s) attempt %d", worker, exc, attempt + 1)
                await asyncio.sleep(1.5)
        _mark(worker, False)
    return text_list


# --------------------------------------------------------------------------- #
# EPUB processing
# --------------------------------------------------------------------------- #
def collect_text_nodes(soup: BeautifulSoup) -> List[NavigableString]:
    """Every translatable text node, in document order. Structure stays intact."""
    nodes: List[NavigableString] = []
    body = soup.body or soup
    for node in body.descendants:
        if not isinstance(node, NavigableString) or isinstance(node, Comment):
            continue
        text = str(node)
        if not text.strip() or _NON_TEXT.match(text.strip()):
            continue
        if any(p.name in SKIP_TAGS for p in node.parents if getattr(p, "name", None)):
            continue
        nodes.append(node)
    return nodes


def make_batches(texts: List[str]) -> List[List[int]]:
    batches: List[List[int]] = []
    cur: List[int] = []
    chars = 0
    for i, t in enumerate(texts):
        if cur and (len(cur) >= BATCH_SIZE or chars + len(t) > BATCH_CHARS):
            batches.append(cur)
            cur, chars = [], 0
        cur.append(i)
        chars += len(t)
    if cur:
        batches.append(cur)
    return batches


def _preserve_ws(original: str, translated: str) -> str:
    """Keep the leading/trailing whitespace of the original node."""
    lead = original[: len(original) - len(original.lstrip())]
    trail = original[len(original.rstrip()):]
    return f"{lead}{translated.strip()}{trail}"


async def translate_texts(session: aiohttp.ClientSession, texts: List[str], target_lang: str,
                          sem: asyncio.Semaphore, counter: dict) -> List[str]:
    results: List[str] = list(texts)
    batches = make_batches(texts)

    async def run(bi: int, idxs: List[int]) -> None:
        async with sem:
            out = await translate_batch_req(session, [texts[i] for i in idxs], target_lang, bi)
        for i, t in zip(idxs, out):
            results[i] = t
        counter["done"] += len(idxs)

    await asyncio.gather(*(run(bi, b) for bi, b in enumerate(batches)))
    return results


async def perform_translation(user_id: int, file_path: str, target_lang: str,
                              original_name: str, status_msg) -> None:
    book = epub.read_epub(file_path, options={"ignore_ncx": False})
    docs = [it for it in book.get_items() if it.get_type() == ebooklib.ITEM_DOCUMENT]

    # Pass 1 – parse all docs & collect nodes (so we know the total for progress)
    parsed: List[Tuple[object, BeautifulSoup, List[NavigableString]]] = []
    total_nodes = 0
    for item in docs:
        soup = BeautifulSoup(item.get_content(), "html.parser")
        nodes = collect_text_nodes(soup)
        parsed.append((item, soup, nodes))
        total_nodes += len(nodes)

    if total_nodes == 0:
        raise RuntimeError("Is EPUB mein koi translatable text nahi mila.")

    counter = {"done": 0}
    last_edit = 0.0
    sem = asyncio.Semaphore(MAX_PARALLEL_BATCHES)
    started = time.time()

    async def progress_loop() -> None:
        nonlocal last_edit
        while True:
            await asyncio.sleep(4)
            pct = int(counter["done"] * 100 / total_nodes)
            elapsed = int(time.time() - started)
            if time.time() - last_edit >= 4:
                try:
                    await status_msg.edit_text(
                        f"⏳ Translating to {LANGUAGES.get(target_lang, target_lang)}: {pct}%\n"
                        f"Segments: {counter['done']}/{total_nodes}\n"
                        f"Workers: {len(healthy_workers())}/{len(WORKERS)} online • {elapsed}s"
                    )
                    last_edit = time.time()
                except Exception:
                    pass

    prog = asyncio.create_task(progress_loop())
    try:
        async with aiohttp.ClientSession(timeout=WORKER_TIMEOUT) as session:
            for item, soup, nodes in parsed:
                if not nodes:
                    continue
                texts = [str(n) for n in nodes]
                translated = await translate_texts(session, texts, target_lang, sem, counter)
                for node, t in zip(nodes, translated):
                    if t and t != str(node):
                        node.replace_with(NavigableString(_preserve_ws(str(node), t)))
                item.set_content(str(soup).encode("utf-8"))
    finally:
        prog.cancel()

    # Update language metadata so readers pick correct fonts/hyphenation
    try:
        book.metadata.setdefault("http://purl.org/dc/elements/1.1/", {})
        book.set_language(target_lang)
    except Exception:
        pass

    base = os.path.splitext(os.path.basename(original_name))[0]
    output_file = f"{base}_{target_lang}.epub"
    epub.write_epub(output_file, book)

    took = int(time.time() - started)
    await status_msg.edit_text("✅ Translation complete! Uploading file...")
    try:
        await app.send_document(
            chat_id=user_id,
            document=output_file,
            caption=(f"📚 {base}\n🌐 {LANGUAGES.get(target_lang, target_lang)} • "
                     f"{total_nodes} segments • {took}s"),
        )
        await status_msg.delete()
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)


async def process_queue() -> None:
    while True:
        user_id, file_path, target_lang, original_name, status_msg = await translation_queue.get()
        try:
            active_tasks[user_id] = "Processing"
            await perform_translation(user_id, file_path, target_lang, original_name, status_msg)
            consume_quota(user_id)
        except Exception as exc:
            log.exception("Translation failed for %s", user_id)
            try:
                await status_msg.edit_text(f"❌ Translation failed: {str(exc)[:300]}")
            except Exception:
                pass
        finally:
            active_tasks.pop(user_id, None)
            translation_queue.task_done()
            if os.path.exists(file_path):
                os.remove(file_path)


# --------------------------------------------------------------------------- #
# Handlers
# --------------------------------------------------------------------------- #
@app.on_message(filters.command("start"))
async def start(client, message):
    user_id = message.from_user.id
    get_user(user_id)
    save_data()
    await message.reply_text(
        "Namaste! Main ek Super Fast EPUB Translator Bot hu.\n\n"
        f"Free users: {FREE_DAILY_LIMIT} files/day. Premium: unlimited.\n"
        "Menu use karein ya seedhe ek EPUB file bhejein.",
        reply_markup=get_main_keyboard(user_id),
    )


@app.on_message(filters.document)
async def handle_document(client, message):
    user_id = message.from_user.id
    name = (message.document.file_name or "").lower()
    if not name.endswith(".epub"):
        return await message.reply_text("⚠️ Kripya sirf .epub file bhejein.")
    if not WORKERS:
        return await message.reply_text("⚠️ Koi bhi worker online nahi hai. Owner se contact karein.")
    if user_id in active_tasks or any(t[0] == user_id for t in list(translation_queue._queue)):
        return await message.reply_text("⚠️ Aapki ek file pehle se queue mein hai. Kripya wait karein.")

    allowed, remaining = check_daily_quota(user_id)
    if not allowed:
        return await message.reply_text(
            f"⚠️ Aaj ka free limit ({FREE_DAILY_LIMIT} files) khatam. Unlimited ke liye /pay karein."
        )

    target_lang = get_user(user_id).get("lang", "hi")
    queue_pos = translation_queue.qsize() + 1
    status_msg = await message.reply_text(
        f"📥 File received → {LANGUAGES.get(target_lang, target_lang)}\n"
        f"Position in queue: {queue_pos}\nDownloading..."
    )
    try:
        file_path = await message.download(file_name=f"downloads/{user_id}_{int(time.time())}.epub")
    except Exception as exc:
        return await status_msg.edit_text(f"❌ Download failed: {exc}")

    await status_msg.edit_text(f"✅ Download complete! Waiting in queue (Position: {queue_pos})...")
    await translation_queue.put((user_id, file_path, target_lang, message.document.file_name, status_msg))


@app.on_message(filters.command("pay"))
async def pay_command(client, message):
    user_id = message.from_user.id
    if is_premium(user_id):
        return await message.reply_text("⭐ Aap already premium hain. Unlimited translations enjoy karein!")
    if rzp_client is None:
        return await message.reply_text(
            "💳 Premium ke liye owner se contact karein. (Payment gateway abhi configured nahi hai.)"
        )
    try:
        link = rzp_client.payment_link.create(data={
            "amount": PREMIUM_PRICE_INR * 100,
            "currency": "INR",
            "accept_partial": False,
            "description": "Unlimited Translation Subscription (1 Month)",
            "notify": {"sms": False, "email": False},
            "reminder_enable": False,
            "notes": {"user_id": str(user_id)},
        })
        await message.reply_text(
            f"Please pay ₹{PREMIUM_PRICE_INR} for 1 month unlimited access.\n"
            f"Click here: {link['short_url']}\n\n"
            f"Payment ke baad owner ko screenshot + apna ID `{user_id}` bhejein."
        )
    except Exception as exc:
        await message.reply_text(f"Payment error: {exc}")


@app.on_message(filters.command("premium") & filters.user(OWNER_ID))
async def grant_premium(client, message):
    """/premium <user_id> [days]  – owner grants premium."""
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /premium <user_id> [days=30]")
    uid = int(parts[1])
    days = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 30
    from datetime import timedelta

    u = get_user(uid)
    u["has_subscription"] = True
    u["premium_until"] = (date.today() + timedelta(days=days)).isoformat()
    save_data()
    await message.reply_text(f"✅ Premium granted to {uid} until {u['premium_until']}")
    try:
        await client.send_message(uid, f"⭐ Aapka premium activate ho gaya hai ({days} din).")
    except Exception:
        pass


@app.on_message(filters.command("revoke") & filters.user(OWNER_ID))
async def revoke_premium(client, message):
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /revoke <user_id>")
    u = get_user(int(parts[1]))
    u["has_subscription"] = False
    u["premium_until"] = None
    save_data()
    await message.reply_text("✅ Premium revoked.")


@app.on_message(filters.command("stats") & filters.user(OWNER_ID))
async def stats_cmd(client, message):
    premium = sum(1 for uid in users_db if is_premium(uid) and uid != OWNER_ID)
    lines = [f"👥 Users: {len(users_db)} (premium {premium})",
             f"📦 Queue: {translation_queue.qsize()} • Active: {len(active_tasks)}",
             f"🖥 Workers: {len(healthy_workers())}/{len(WORKERS)} healthy", ""]
    for w in WORKERS:
        s = worker_stats.get(w, {"ok": 0, "fail": 0})
        flag = "🟢" if w in healthy_workers() else "🔴"
        lines.append(f"{flag} {w}  ok={s['ok']} fail={s['fail']}")
    await message.reply_text("\n".join(lines), disable_web_page_preview=True)


def _normalize_url(text: str) -> str:
    url = text.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")


@app.on_message(filters.text & ~filters.command(["start", "pay", "premium", "revoke", "stats"]))
async def handle_text_buttons(client, message):
    user_id = message.from_user.id
    text = message.text.strip()

    # ---- Admin state machine ----
    if user_id == OWNER_ID and user_id in admin_state:
        state = admin_state.pop(user_id)
        if text == "❌ Cancel":
            return await message.reply_text("Action cancelled.", reply_markup=get_main_keyboard(user_id))

        url = _normalize_url(text)
        if state == "ADDING_WORKER":
            if url in WORKERS:
                return await message.reply_text("⚠️ Worker pehle se maujood hai.", reply_markup=get_main_keyboard(user_id))
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                ok, info = await check_worker(session, url)
            if not ok:
                return await message.reply_text(
                    f"❌ Worker reachable nahi hai ({info}).\nURL check karke dobara try karein.",
                    reply_markup=get_main_keyboard(user_id),
                )
            WORKERS.append(url)
            save_data()
            return await message.reply_text(
                f"✅ Worker added ({info})!\nTotal workers: {len(WORKERS)}",
                reply_markup=get_main_keyboard(user_id),
            )

        if state == "DELETING_WORKER":
            # allow deleting by index number too
            if text.isdigit() and 1 <= int(text) <= len(WORKERS):
                url = WORKERS[int(text) - 1]
            if url in WORKERS:
                WORKERS.remove(url)
                save_data()
                return await message.reply_text(
                    f"✅ Worker removed!\nTotal workers left: {len(WORKERS)}",
                    reply_markup=get_main_keyboard(user_id),
                )
            return await message.reply_text("⚠️ Worker list mein nahi mila.", reply_markup=get_main_keyboard(user_id))

    # ---- Menu buttons ----
    if text == "🌐 Set Language":
        buttons = [InlineKeyboardButton(l, callback_data=f"lang_{c}") for c, l in LANGUAGES.items()]
        keyboard = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
        await message.reply_text("Niche apni pasandida bhasha chunein:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif text == "💳 Premium (/pay)":
        await pay_command(client, message)

    elif text == "📊 Queue Status":
        allowed, remaining = check_daily_quota(user_id)
        tier = "⭐ Premium" if is_premium(user_id) else f"Free ({remaining} left today)"
        await message.reply_text(
            f"📊 **System Status**\n\n"
            f"Workers Online: {len(healthy_workers())}/{len(WORKERS)}\n"
            f"Files Processing: {len(active_tasks)}\n"
            f"Files in Queue: {translation_queue.qsize()}\n\n"
            f"Your plan: {tier}\n"
            f"Your language: {LANGUAGES.get(get_user(user_id).get('lang', 'hi'))}"
        )

    elif text == "❓ Help":
        await message.reply_text(
            "1. 🌐 Language set karein.\n"
            "2. 📚 Apni EPUB file upload karein.\n"
            "3. ⏳ Bot translate karke nayi EPUB bhej dega (images, TOC, CSS safe rehte hain).\n\n"
            f"Free: {FREE_DAILY_LIMIT} files/day • Premium: unlimited (/pay)."
        )

    elif text == "➕ Add Worker" and user_id == OWNER_ID:
        admin_state[user_id] = "ADDING_WORKER"
        cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True)
        await message.reply_text("Naye worker ka URL bhejein (eg: https://app.onrender.com):", reply_markup=cancel_kb)

    elif text == "➖ Del Worker" and user_id == OWNER_ID:
        if not WORKERS:
            return await message.reply_text("Koi worker list mein nahi hai.")
        admin_state[user_id] = "DELETING_WORKER"
        cancel_kb = ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True)
        listing = "\n".join(f"{i + 1}. {w}" for i, w in enumerate(WORKERS))
        await message.reply_text(f"Worker ka URL ya number bhejein jise hatana hai:\n\n{listing}", reply_markup=cancel_kb)

    elif text == "🖥 Worker List" and user_id == OWNER_ID:
        if not WORKERS:
            return await message.reply_text("Koi worker list mein nahi hai.")
        msg = await message.reply_text("🔍 Checking workers...")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            checks = await asyncio.gather(*(check_worker(session, w) for w in WORKERS))
        lines = []
        for i, (w, (ok, info)) in enumerate(zip(WORKERS, checks)):
            if ok:
                worker_failures.pop(w, None)
            lines.append(f"{i + 1}. {'🟢' if ok else '🔴'} {w}\n    {info}")
        await msg.edit_text("Current Workers:\n\n" + "\n".join(lines), disable_web_page_preview=True)

    elif text == "🧹 Clear Stuck Tasks" and user_id == OWNER_ID:
        active_tasks.clear()
        worker_failures.clear()
        await message.reply_text("✅ Stuck tasks aur worker cooldowns clear kar diye gaye.")


@app.on_callback_query(filters.regex(r"^lang_"))
async def set_language(client, callback_query):
    lang_code = callback_query.data.split("_", 1)[1]
    if lang_code not in LANGUAGES:
        return await callback_query.answer("Unknown language", show_alert=True)
    user_id = callback_query.from_user.id
    get_user(user_id)["lang"] = lang_code
    save_data()
    await callback_query.answer(f"Language {LANGUAGES[lang_code]} set ho gayi hai.")
    await callback_query.message.edit_text(
        f"Selected Language: **{LANGUAGES[lang_code]}**\nAb apni EPUB file bhej sakte hain."
    )


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
async def main() -> None:
    os.makedirs("downloads", exist_ok=True)
    await app.start()
    asyncio.create_task(keep_workers_alive())
    asyncio.create_task(process_queue())
    log.info("Master Bot started. Workers: %d, users: %d", len(WORKERS), len(users_db))
    await idle()
    await app.stop()


if __name__ == "__main__":
    if not (API_ID and API_HASH and BOT_TOKEN):
        raise SystemExit("API_ID, API_HASH aur BOT_TOKEN environment variables set karein (.env dekhein).")
    app.run(main())
