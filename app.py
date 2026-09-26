"""
Translation Worker  (Render / Vercel / any free-tier host)
==========================================================
Lightweight FastAPI micro-service used by the Master Bot (bot.py).
It receives a batch of text strings and returns translations.

Endpoints
---------
GET  /            -> keep-alive ping (used by master every few minutes)
GET  /health      -> detailed health JSON
POST /translate   -> {"text_list": [...], "lang": "hi", "source": "auto"}
                  <- {"success": true, "translated": [...], "failed": 0, "took_ms": 123}

Environment
-----------
PORT             (Render sets this automatically)
WORKER_SECRET    optional shared secret. If set, requests must send
                 header  X-Worker-Secret: <value>
MAX_CONCURRENCY  parallel upstream requests (default 6)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Any, List, Optional

import aiohttp
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
WORKER_SECRET = os.environ.get("WORKER_SECRET", "").strip()
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "6"))
MAX_ITEMS_PER_REQ = 40          # texts per Google request
MAX_CHARS_PER_REQ = 4000        # total chars per Google request
MAX_SINGLE_TEXT = 3500          # longer texts are split into sentences
UPSTREAM_TIMEOUT = aiohttp.ClientTimeout(total=20)
RETRIES = 3

GOOGLE_URL = "https://translate.googleapis.com/translate_a/t"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

app = FastAPI(title="EPUB Translator Worker", version="2.0.0", docs_url=None, redoc_url=None)

_semaphore: Optional[asyncio.Semaphore] = None
_session: Optional[aiohttp.ClientSession] = None
_stats = {"requests": 0, "texts": 0, "chars": 0, "errors": 0, "started": time.time()}


def _sem() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    return _semaphore


async def _get_session() -> aiohttp.ClientSession:
    """Re-usable session (created lazily so it works on serverless too)."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(timeout=UPSTREAM_TIMEOUT, headers=HEADERS)
    return _session


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _session and not _session.closed:
        await _session.close()


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class TranslateIn(BaseModel):
    text_list: List[str] = Field(default_factory=list)
    lang: str = "hi"
    source: str = "auto"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
_SENT_SPLIT = re.compile(r"(?<=[.!?।॥\n])\s+")


def split_long_text(text: str, limit: int = MAX_SINGLE_TEXT) -> List[str]:
    """Split very long text at sentence boundaries so each piece <= limit."""
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    buf = ""
    for sentence in _SENT_SPLIT.split(text):
        if not sentence:
            continue
        if len(buf) + len(sentence) + 1 > limit and buf:
            parts.append(buf)
            buf = sentence
        else:
            buf = f"{buf} {sentence}" if buf else sentence
        # Hard split for a single gigantic "sentence"
        while len(buf) > limit:
            parts.append(buf[:limit])
            buf = buf[limit:]
    if buf:
        parts.append(buf)
    return parts


def _parse_google(result: Any, expected: int) -> Optional[List[str]]:
    """
    Google returns either:
      ["t1", "t2"]                       (when source lang given)
      [["t1","en"], ["t2","en"]]         (when source = auto)
      "t1"  or ["t1","en"]               (single item edge-cases)
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


async def _google_request(texts: List[str], target: str, source: str) -> Optional[List[str]]:
    """One upstream call with retries. Returns None if permanently failed."""
    session = await _get_session()
    params = {"client": "gtx", "sl": source or "auto", "tl": target}
    payload = [("q", t) for t in texts]
    for attempt in range(RETRIES):
        try:
            async with _sem():
                async with session.post(GOOGLE_URL, params=params, data=payload) as resp:
                    if resp.status == 200:
                        parsed = _parse_google(await resp.json(content_type=None), len(texts))
                        if parsed is not None:
                            return parsed
                        log.warning("Unexpected upstream shape for %d texts", len(texts))
                    elif resp.status in (429, 503):
                        log.warning("Upstream rate-limited (%s), attempt %d", resp.status, attempt + 1)
                    else:
                        log.warning("Upstream HTTP %s", resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning("Upstream error: %s (attempt %d)", exc, attempt + 1)
        await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def _translate_chunk(texts: List[str], target: str, source: str) -> List[Optional[str]]:
    """Translate one chunk; on failure split in half; single failures return None."""
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


async def translate_texts(text_list: List[str], target: str, source: str = "auto") -> tuple[List[str], int]:
    """
    Translate a list of strings preserving order.
    Returns (translated_list, failed_count). Failed items fall back to the original text.
    """
    # 1) Expand long texts into pieces, remember mapping.
    pieces: List[str] = []
    owner: List[int] = []           # piece index -> original index
    for idx, text in enumerate(text_list):
        if not text or not text.strip():
            continue
        for p in split_long_text(text):
            pieces.append(p)
            owner.append(idx)

    # 2) Chunk pieces by limits.
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

    # 3) Translate chunks concurrently.
    results: List[Optional[str]] = [None] * len(pieces)

    async def run(chunk: List[int]) -> None:
        out = await _translate_chunk([pieces[i] for i in chunk], target, source)
        for i, t in zip(chunk, out):
            results[i] = t

    await asyncio.gather(*(run(c) for c in chunks))

    # 4) Re-assemble.
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


@app.get("/")
async def root() -> dict:
    return {"status": "ok", "service": "translator-worker", "uptime_s": int(time.time() - _stats["started"])}


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": app.version,
        "secured": bool(WORKER_SECRET),
        "max_concurrency": MAX_CONCURRENCY,
        "uptime_s": int(time.time() - _stats["started"]),
        **{k: v for k, v in _stats.items() if k != "started"},
    }


@app.post("/translate")
async def translate_endpoint(
    body: TranslateIn,
    x_worker_secret: Optional[str] = Header(default=None),
) -> dict:
    _check_secret(x_worker_secret)
    if not body.text_list:
        return {"success": True, "translated": [], "failed": 0, "took_ms": 0}
    if len(body.text_list) > 500:
        raise HTTPException(status_code=413, detail="Too many items (max 500 per request)")

    target = (body.lang or "hi").strip().lower()
    started = time.perf_counter()
    _stats["requests"] += 1
    _stats["texts"] += len(body.text_list)
    _stats["chars"] += sum(len(t) for t in body.text_list)
    try:
        translated, failed = await translate_texts(body.text_list, target, body.source)
    except Exception as exc:  # never crash the worker
        _stats["errors"] += 1
        log.exception("translate failed")
        return {"success": False, "error": str(exc)}
    took = int((time.perf_counter() - started) * 1000)
    return {"success": True, "translated": translated, "failed": failed, "took_ms": took}


@app.exception_handler(Exception)
async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
    log.exception("Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={"success": False, "error": "internal error"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="info")
