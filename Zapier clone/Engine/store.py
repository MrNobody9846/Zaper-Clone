"""Filesystem-backed JSON store for nodes, workflows, and jobs.

Writes go through a temp file + atomic rename so a concurrent reader (e.g.
`wf list` in another terminal) never sees a half-written file.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .models import Job, Node, Workflow


# Path to the directory containing the Db/ folder. By default it lives next to
# the Engine/ package (i.e. at the repo root), but tests can override via the
# WF_DB_ROOT environment variable.
def _default_root() -> Path:
    env = os.environ.get("WF_DB_ROOT")
    if env:
        return Path(env).resolve()
    return Path(__file__).resolve().parent.parent / "Db"


class Store:
    """Read/write JSON entities under <root>/{nodes,workflows,jobs}/."""

    def __init__(self, root: Optional[Path] = None) -> None:
        self.root = (root or _default_root()).resolve()
        self.nodes_dir = self.root / "nodes"
        self.workflows_dir = self.root / "workflows"
        self.jobs_dir = self.root / "jobs"
        for d in (self.nodes_dir, self.workflows_dir, self.jobs_dir):
            d.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    @staticmethod
    def _atomic_write(path: Path, payload: Dict) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=False)
            fh.write("\n")
        os.replace(tmp, path)

    @staticmethod
    def _read_json(path: Path) -> Dict:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    # ---- Nodes ----------------------------------------------------------

    def load_node(self, node_id: str) -> Node:
        path = self.nodes_dir / f"{node_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"Node '{node_id}' not found at {path}")
        return Node.from_dict(self._read_json(path))

    def save_node(self, node: Node) -> None:
        with self._lock:
            self._atomic_write(self.nodes_dir / f"{node.id}.json", node.to_dict())

    def list_nodes(self) -> List[Node]:
        return [Node.from_dict(self._read_json(p)) for p in sorted(self.nodes_dir.glob("*.json"))]

    # ---- Workflows ------------------------------------------------------

    def load_workflow(self, workflow_id: str) -> Workflow:
        path = self.workflows_dir / f"{workflow_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"Workflow '{workflow_id}' not found at {path}")
        return Workflow.from_dict(self._read_json(path))

    def save_workflow(self, workflow: Workflow) -> None:
        with self._lock:
            self._atomic_write(
                self.workflows_dir / f"{workflow.id}.json", workflow.to_dict()
            )

    def list_workflows(self) -> List[Workflow]:
        return [
            Workflow.from_dict(self._read_json(p))
            for p in sorted(self.workflows_dir.glob("*.json"))
        ]

    # ---- Jobs -----------------------------------------------------------

    def load_job(self, job_id: str) -> Job:
        path = self.jobs_dir / f"{job_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"Job '{job_id}' not found at {path}")
        return Job.from_dict(self._read_json(path))

    def save_job(self, job: Job) -> None:
        with self._lock:
            self._atomic_write(self.jobs_dir / f"{job.id}.json", job.to_dict())

    def list_jobs(self) -> List[Job]:
        jobs: List[Job] = []
        for p in self.jobs_dir.glob("*.json"):
            try:
                jobs.append(Job.from_dict(self._read_json(p)))
            except (json.JSONDecodeError, KeyError):
                continue
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs

    def iter_jobs_for_workflow(self, workflow_id: str) -> Iterable[Job]:
        for job in self.list_jobs():
            if job.workflow_id == workflow_id:
                yield job
