"""
Worker pool – talks to the translation workers (app.py) on Render / Vercel.

* Least-loaded routing: each batch goes to the healthy worker with the fewest
  in-flight requests (ties broken by measured latency).
* Failover: on any error the batch is retried on the next worker; a failing
  worker is put on cooldown so it does not slow down every batch.
* Keep-alive: pings every worker periodically so free-tier hosts stay awake.
* Never raises into the caller except `NoWorkersError` when the pool is empty.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple

import aiohttp

from .config import settings
from .store import Store

log = logging.getLogger("workers")


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
