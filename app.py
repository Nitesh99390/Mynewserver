"""
Translation Worker  v3  (Render / Vercel / Railway / any free-tier host)
========================================================================
Stateless FastAPI micro-service used by the Master Bot (bot.py).
It receives a batch of strings and returns their translations, in order.

Endpoints
---------
GET  /            keep-alive ping                      -> {"status":"ok", ...}
GET  /health      detailed health / load JSON          (used for load-balancing)
POST /translate   {"text_list":[...], "lang":"hi", "source":"auto"}
                  -> {"success":true, "translated":[...], "failed":0, "took_ms":57}

Environment
-----------
PORT               set automatically by Render / Railway
WORKER_SECRET      optional shared secret. If set, requests must carry
                   header  X-Worker-Secret: <value>
MAX_CONCURRENCY    parallel upstream requests (default 8)
MAX_INFLIGHT       max simultaneous /translate requests before 429 (default 16)
UPSTREAM_TIMEOUT   seconds per upstream call (default 20)

Deploy
------
Render  : Build `pip install -r requirements.txt`, Start `python app.py`  (render.yaml included)
Vercel  : `vercel.json` routes everything to this file (api/index.py re-exports `app`)
Local   : `PORT=8000 python app.py`

Design notes
------------
* Never crashes: every code path returns JSON; unexpected errors -> 500 JSON.
* Upstream failures degrade gracefully: chunk is halved and retried; single
  items that still fail are returned untranslated and counted in "failed".
* Two independent Google endpoints are used (primary + fallback) so a
  temporary block on one does not stop the worker.
* Works on serverless (Vercel) because the HTTP session is created lazily.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Any, List, Optional, Sequence, Tuple

import aiohttp
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
VERSION = "3.0.0"
WORKER_SECRET = os.environ.get("WORKER_SECRET", "").strip()
MAX_CONCURRENCY = max(1, int(os.environ.get("MAX_CONCURRENCY", "8")))
MAX_INFLIGHT = max(1, int(os.environ.get("MAX_INFLIGHT", "16")))
UPSTREAM_TIMEOUT_S = float(os.environ.get("UPSTREAM_TIMEOUT", "20"))

MAX_ITEMS_PER_REQ = 40          # texts per upstream request
MAX_CHARS_PER_REQ = 4500        # chars per upstream request
MAX_SINGLE_TEXT = 3500          # longer texts are split at sentence boundaries
MAX_ITEMS_PER_CALL = 500        # hard cap per /translate call
MAX_CHARS_PER_CALL = 60_000     # hard cap per /translate call
RETRIES = 3

PRIMARY_URL = "https://translate.googleapis.com/translate_a/t"
FALLBACK_URL = "https://clients5.google.com/translate_a/t"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

# --------------------------------------------------------------------------- #
# State (process-local; fine for stateless workers)
# --------------------------------------------------------------------------- #
_semaphore: Optional[asyncio.Semaphore] = None
_session: Optional[aiohttp.ClientSession] = None
_stats = {
    "requests": 0, "texts": 0, "chars": 0, "errors": 0, "upstream_calls": 0,
    "upstream_fail": 0, "inflight": 0, "rejected_busy": 0, "started": time.time(),
    "last_ok": 0.0, "last_error": "",
}


def _sem() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    return _semaphore


async def _get_session() -> aiohttp.ClientSession:
    """Re-usable HTTP session, created lazily so it also works on serverless."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=UPSTREAM_TIMEOUT_S),
            headers=HEADERS,
            connector=aiohttp.TCPConnector(limit=MAX_CONCURRENCY * 2, ttl_dns_cache=300),
        )
    return _session


@asynccontextmanager
async def _lifespan(_: FastAPI):
    log.info("Worker v%s starting (concurrency=%d, secured=%s)", VERSION, MAX_CONCURRENCY, bool(WORKER_SECRET))
    yield
    if _session and not _session.closed:
        await _session.close()


app = FastAPI(title="EPUB Translator Worker", version=VERSION, docs_url=None, redoc_url=None,
              lifespan=_lifespan)


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class TranslateIn(BaseModel):
    text_list: List[str] = Field(default_factory=list)
    lang: str = "hi"
    source: str = "auto"


# --------------------------------------------------------------------------- #
# Text helpers
# --------------------------------------------------------------------------- #
_SENT_SPLIT = re.compile(r"(?<=[.!?।॥\n])\s+")
_LANG_RE = re.compile(r"^[a-zA-Z]{2,3}(-[a-zA-Z]{2,4})?$")


def split_long_text(text: str, limit: int = MAX_SINGLE_TEXT) -> List[str]:
    """Split a very long string at sentence boundaries so each piece <= limit."""
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    buf = ""
    for sentence in _SENT_SPLIT.split(text):
        if not sentence:
            continue
        if buf and len(buf) + len(sentence) + 1 > limit:
            parts.append(buf)
            buf = sentence
        else:
            buf = f"{buf} {sentence}" if buf else sentence
        while len(buf) > limit:              # single gigantic "sentence"
            parts.append(buf[:limit])
            buf = buf[limit:]
    if buf:
        parts.append(buf)
    return parts


def _parse_google(result: Any, expected: int) -> Optional[List[str]]:
    """
    Google's /translate_a/t returns one of:
      ["t1", "t2"]                    (source language given)
      [["t1","en"], ["t2","en"]]      (source = auto)
      "t1"  /  ["t1","en"]            (single-item edge cases)
    """
    if isinstance(result, str):
        out = [result]
    elif isinstance(result, list):
        if expected == 1 and len(result) == 2 and all(isinstance(x, str) for x in result):
            out = [result[0]]
        else:
            out = []
            for item in result:
                if isinstance(item, list) and item:
                    out.append(str(item[0]))
                elif isinstance(item, str):
                    out.append(item)
                else:
                    out.append("")
    else:
        return None
    return out if len(out) == expected else None


# --------------------------------------------------------------------------- #
# Upstream
# --------------------------------------------------------------------------- #
async def _google_request(texts: Sequence[str], target: str, source: str) -> Optional[List[str]]:
    """One logical upstream call with retries + endpoint fallback. None = failed."""
    session = await _get_session()
    params = {"client": "gtx", "sl": source or "auto", "tl": target, "format": "text"}
    payload = [("q", t) for t in texts]
    for attempt in range(RETRIES):
        url = PRIMARY_URL if attempt < RETRIES - 1 else FALLBACK_URL
        _stats["upstream_calls"] += 1
        try:
            async with _sem():
                async with session.post(url, params=params, data=payload) as resp:
                    if resp.status == 200:
                        parsed = _parse_google(await resp.json(content_type=None), len(texts))
                        if parsed is not None:
                            _stats["last_ok"] = time.time()
                            return parsed
                        log.warning("Unexpected upstream shape for %d texts", len(texts))
                    elif resp.status in (429, 503, 502):
                        log.warning("Upstream %s busy (%s) attempt %d", url, resp.status, attempt + 1)
                    else:
                        _stats["last_error"] = f"HTTP {resp.status}"
                        log.warning("Upstream %s HTTP %s", url, resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            _stats["last_error"] = type(exc).__name__
            log.warning("Upstream error: %s (attempt %d)", exc, attempt + 1)
        _stats["upstream_fail"] += 1
        await asyncio.sleep(1.0 * (attempt + 1))
    return None


async def _translate_chunk(texts: List[str], target: str, source: str) -> List[Optional[str]]:
    """Translate one chunk; on failure split in half; single failures -> None."""
    res = await _google_request(texts, target, source)
    if res is not None:
        return res
    if len(texts) == 1:
        return [None]
    mid = len(texts) // 2
    left, right = await asyncio.gather(
        _translate_chunk(texts[:mid], target, source),
        _translate_chunk(texts[mid:], target, source),
    )
    return left + right


async def translate_texts(text_list: List[str], target: str, source: str = "auto") -> Tuple[List[str], int]:
    """
    Translate a list of strings preserving order.
    Returns (translated_list, failed_count). Failed items fall back to the original.
    """
    pieces: List[str] = []
    owner: List[int] = []
    for idx, text in enumerate(text_list):
        if not text or not text.strip():
            continue
        for p in split_long_text(text):
            pieces.append(p)
            owner.append(idx)

    chunks: List[List[int]] = []
    cur: List[int] = []
    cur_chars = 0
    for pi, p in enumerate(pieces):
        if cur and (len(cur) >= MAX_ITEMS_PER_REQ or cur_chars + len(p) > MAX_CHARS_PER_REQ):
            chunks.append(cur)
            cur, cur_chars = [], 0
        cur.append(pi)
        cur_chars += len(p)
    if cur:
        chunks.append(cur)

    results: List[Optional[str]] = [None] * len(pieces)

    async def run(chunk: List[int]) -> None:
        out = await _translate_chunk([pieces[i] for i in chunk], target, source)
        for i, t in zip(chunk, out):
            results[i] = t

    await asyncio.gather(*(run(c) for c in chunks))

    assembled: List[List[str]] = [[] for _ in text_list]
    failed_flags = [False] * len(text_list)
    for pi, t in enumerate(results):
        oi = owner[pi]
        if t is None:
            failed_flags[oi] = True
            assembled[oi].append(pieces[pi])
        else:
            assembled[oi].append(t)

    final: List[str] = []
    failed = 0
    for idx, original in enumerate(text_list):
        if not assembled[idx]:
            final.append(original)
            continue
        if failed_flags[idx]:
            failed += 1
        final.append(" ".join(assembled[idx]))
    return final, failed


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
def _check_secret(secret: Optional[str]) -> None:
    if WORKER_SECRET and secret != WORKER_SECRET:
        raise HTTPException(status_code=401, detail="Invalid worker secret")


def _health_payload() -> dict:
    return {
        "status": "ok",
        "version": VERSION,
        "secured": bool(WORKER_SECRET),
        "max_concurrency": MAX_CONCURRENCY,
        "max_inflight": MAX_INFLIGHT,
        "uptime_s": int(time.time() - _stats["started"]),
        "load": round(_stats["inflight"] / MAX_INFLIGHT, 2),
        **{k: v for k, v in _stats.items() if k not in ("started",)},
    }


@app.head("/")
async def root_head() -> JSONResponse:
    return JSONResponse(content={}, status_code=200)


@app.get("/")
async def root() -> dict:
    return {"status": "ok", "service": "translator-worker", "version": VERSION,
            "uptime_s": int(time.time() - _stats["started"]), "inflight": _stats["inflight"]}


@app.get("/health")
async def health() -> dict:
    return _health_payload()


@app.post("/translate")
async def translate_endpoint(
    body: TranslateIn,
    x_worker_secret: Optional[str] = Header(default=None),
) -> Any:
    _check_secret(x_worker_secret)
    if not body.text_list:
        return {"success": True, "translated": [], "failed": 0, "took_ms": 0}
    if len(body.text_list) > MAX_ITEMS_PER_CALL:
        raise HTTPException(status_code=413, detail=f"Too many items (max {MAX_ITEMS_PER_CALL})")
    total_chars = sum(len(t) for t in body.text_list)
    if total_chars > MAX_CHARS_PER_CALL:
        raise HTTPException(status_code=413, detail=f"Too many chars (max {MAX_CHARS_PER_CALL})")
    if _stats["inflight"] >= MAX_INFLIGHT:
        _stats["rejected_busy"] += 1
        return JSONResponse(status_code=429, content={"success": False, "error": "busy",
                                                      "inflight": _stats["inflight"]})

    target = (body.lang or "hi").strip().lower()
    if not _LANG_RE.match(target):
        raise HTTPException(status_code=422, detail="Invalid target language code")
    source = (body.source or "auto").strip().lower() or "auto"

    started = time.perf_counter()
    _stats["requests"] += 1
    _stats["texts"] += len(body.text_list)
    _stats["chars"] += total_chars
    _stats["inflight"] += 1
    try:
        translated, failed = await translate_texts(body.text_list, target, source)
    except Exception as exc:  # never crash the worker
        _stats["errors"] += 1
        log.exception("translate failed")
        return {"success": False, "error": str(exc)[:200]}
    finally:
        _stats["inflight"] -= 1
    took = int((time.perf_counter() - started) * 1000)
    return {"success": True, "translated": translated, "failed": failed, "took_ms": took}


@app.exception_handler(Exception)
async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
    log.exception("Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={"success": False, "error": "internal error"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info",
                proxy_headers=True, forwarded_allow_ips="*", timeout_keep_alive=30)
