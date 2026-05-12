"""Orchestrator engine: schedules workflow jobs onto a thread pool.

`max_instances` is the cap on concurrent running jobs. Anything submitted
beyond that sits in the pool's internal queue until a slot frees up. The
on-disk job JSON is flipped to `running` only when a worker actually picks it
up, so `wf list --status queued` accurately reflects the waiting queue.

Fan-out children are submitted through the same `Engine.submit`, so they share
the same concurrency cap. The parent job releases its slot immediately after
submitting children (the executor sets `current = None`), so children can grab
slots without deadlock.
"""

from __future__ import annotations

import threading
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

from .executor import WorkflowExecutor
from .models import Job, JobStatus
from .store import Store


class Engine:
    def __init__(
        self,
        max_instances: int = 5,
        store: Optional[Store] = None,
        *,
        force_fire: bool = False,
    ) -> None:
        if max_instances < 1:
            raise ValueError("max_instances must be >= 1")
        self.max_instances = max_instances
        self.store = store or Store()
        self.force_fire = force_fire
        self._pool = ThreadPoolExecutor(max_workers=max_instances)
        self._executor = WorkflowExecutor(self, self.store)
        self._futures_lock = threading.Lock()
        self._futures: List[Future] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(
        self,
        workflow_id: str,
        *,
        input: Optional[Dict[str, Any]] = None,
        parent_job_id: Optional[str] = None,
        override_start: Optional[str] = None,
    ) -> str:
        """Queue a workflow instance for execution and return its job id."""
        job_input: Dict[str, Any] = dict(input or {})
        if override_start:
            job_input["__override_start__"] = override_start

        job = Job(
            id=Job.new_id(),
            workflow_id=workflow_id,
            status=JobStatus.QUEUED,
            input=job_input,
            parent_job_id=parent_job_id,
        )
        self.store.save_job(job)

        future = self._pool.submit(self._run_job, job.id)
        future.add_done_callback(_make_done_logger(job.id))
        with self._futures_lock:
            self._futures.append(future)
        return job.id

    def wait_all(self, timeout: Optional[float] = None) -> None:
        """Block until every submitted future (including ones spawned during
        execution by fan-out) has finished."""
        deadline = (time.time() + timeout) if timeout is not None else None
        while True:
            with self._futures_lock:
                pending = [f for f in self._futures if not f.done()]
                self._futures = [f for f in self._futures if not f.done()] + [
                    f for f in self._futures if f.done()
                ]
            if not pending:
                with self._futures_lock:
                    if all(f.done() for f in self._futures):
                        return
                continue
            for f in pending:
                if deadline is not None:
                    remaining = max(0.0, deadline - time.time())
                    try:
                        f.result(timeout=remaining)
                    except Exception:
                        pass
                else:
                    try:
                        f.result()
                    except Exception:
                        pass
            if deadline is not None and time.time() >= deadline:
                return

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.shutdown(wait=True)

    # ------------------------------------------------------------------
    # Worker entry point
    # ------------------------------------------------------------------

    def _run_job(self, job_id: str) -> None:
        """Run a single job. This is the seam where a Docker `run` call would
        replace the in-process executor in a future revision."""
        try:
            job = self.store.load_job(job_id)
        except Exception:
            traceback.print_exc()
            return
        self._executor.run(job, force_fire=self.force_fire)


def _make_done_logger(job_id: str) -> Callable[[Future], None]:
    def _cb(future: Future) -> None:
        exc = future.exception()
        if exc is not None:
            traceback.print_exception(type(exc), exc, exc.__traceback__)
    return _cb
