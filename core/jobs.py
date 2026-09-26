"""
Job manager – pending confirmations, a bounded queue and N concurrent runners.

Flow
----
prepare()  download done -> analyse (thread) -> Pending (waits for user tap)
enqueue()  user confirmed & paid          -> Job in queue
runner     picks Job -> epub_engine.translate -> callbacks -> cleanup/refund
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, List, Optional

from . import epub_engine
from .config import settings
from .epub_engine import Analysis
from .store import Store
from .workers import NoWorkersError, WorkerPool

log = logging.getLogger("jobs")


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
            epub_engine.cleanup(p.path)
        for job in list(self._queued.values()):
            self.store.refund(job.user_id, job.breakdown)
            self.store.update_job(job.id, "cancelled", "shutdown")
            epub_engine.cleanup(job.path)

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
        analysis = await asyncio.to_thread(epub_engine.analyse, path)
        token = secrets.token_urlsafe(8)
        p = Pending(token, uid, chat_id, msg_id, path, file_name, lang, analysis)
        self.pending[token] = p
        return p

    def drop_pending(self, token: str) -> Optional[Pending]:
        p = self.pending.pop(token, None)
        if p:
            epub_engine.cleanup(p.path)
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
            out = await epub_engine.translate(job.analysis, job.lang, self.pool.translate, progress,
                                              job.cancel, settings.download_dir)
        except asyncio.CancelledError:
            if job.cancel.is_set():
                return await self._finish(job, None, "cancelled")
            raise
        except NoWorkersError:
            return await self._finish(job, None, "Koi translation worker online nahi hai.")
        except epub_engine.EpubError as exc:
            return await self._finish(job, None, str(exc))
        except Exception as exc:
            log.exception("job %s failed", job.id)
            return await self._finish(job, None, f"{type(exc).__name__}: {str(exc)[:120]}")

        job.progress.done = job.progress.total
        job.progress.failed_segments = getattr(job.analysis, "failed_segments", 0)
        if job.progress.failed_segments and job.progress.failed_segments >= job.analysis.total_nodes:
            epub_engine.cleanup(out)
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
        epub_engine.cleanup(job.path, out_path)

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
                                epub_engine.cleanup(fp)
                        except OSError:
                            pass
            except Exception as exc:  # pragma: no cover
                log.debug("janitor: %s", exc)
