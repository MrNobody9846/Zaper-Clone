"""Dataclasses describing nodes, workflows, and jobs.

Every entity round-trips through plain JSON so the on-disk representation stays
human-readable and editable.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class NodeType(str, Enum):
    TRIGGER = "trigger"
    ACTION = "action"
    CONDITION = "condition"


class TriggerType(str, Enum):
    MANUAL = "manual"
    SCHEDULE = "schedule"  # not needed right now
    FILE_WATCH = "file_watch" # not needed right now
    WEBHOOK = "webhook" # not needed right now


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Node:
    """A reusable node definition stored under Db/nodes/<id>.json."""

    id: str
    type: NodeType
    # Action-only fields
    action: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    # Trigger-only fields
    trigger_type: Optional[TriggerType] = None
    # Condition-only fields
    metric: Optional[str] = None
    input: Optional[str] = None
    operator: Optional[str] = None
    value: Any = None
    word: Optional[str] = None
    # Multi-way mode: when `buckets` is set, the condition emits a label string
    # (the first matching bucket) instead of a bool. Each bucket may declare
    # `min` (inclusive) and/or `max` (exclusive); a bucket with neither acts as
    # the default catch-all.
    buckets: Optional[List[Dict[str, Any]]] = None
    # Condition routing: maps result-label -> child node id.
    # For boolean conditions the labels are "true"/"false"; for bucket
    # conditions the labels match the bucket `label` field. Every value here
    # must also appear in the workflow's children[<this_node>] list.
    branches: Optional[Dict[str, str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Node":
        branches = data.get("branches")
        buckets = data.get("buckets")
        return cls(
            id=data["id"],
            type=NodeType(data["type"]),
            action=data.get("action"),
            params=data.get("params", {}) or {},
            trigger_type=(
                TriggerType(data["trigger_type"])
                if data.get("trigger_type") is not None
                else None
            ),
            metric=data.get("metric"),
            input=data.get("input"),
            operator=data.get("operator"),
            value=data.get("value"),
            word=data.get("word"),
            buckets=[dict(b) for b in buckets] if buckets else None,
            branches=dict(branches) if branches else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"id": self.id, "type": self.type.value}
        if self.type is NodeType.ACTION:
            out["action"] = self.action
            out["params"] = self.params
        elif self.type is NodeType.TRIGGER:
            if self.trigger_type is not None:
                out["trigger_type"] = self.trigger_type.value
            if self.params:
                out["params"] = self.params
        elif self.type is NodeType.CONDITION:
            out["metric"] = self.metric
            if self.input is not None:
                out["input"] = self.input
            if self.buckets:
                out["buckets"] = self.buckets
            else:
                out["operator"] = self.operator
                out["value"] = self.value
            if self.word is not None:
                out["word"] = self.word
            if self.branches:
                out["branches"] = self.branches
        return out


@dataclass
class Workflow:
    """A directed graph of node ids stored under Db/workflows/<id>.json.

    `children` is a pure adjacency list: ``Dict[parent_id, List[child_id]]``.
    An empty list marks a terminal node. Condition routing (which child is the
    true branch and which is the false branch) lives on the condition node
    itself in `Node.branches` — the values there must also appear in this map
    under the same parent id.
    """

    id: str
    name: str
    start: str
    children: Dict[str, List[str]]
    fanout: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Workflow":
        children = _coerce_children(data)
        return cls(
            id=data["id"],
            name=data.get("name", data["id"]),
            start=data["start"],
            children=children,
            fanout=data.get("fanout"),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "id": self.id,
            "name": self.name,
            "start": self.start,
            "children": {k: list(v) for k, v in self.children.items()},
        }
        if self.fanout:
            out["fanout"] = self.fanout
        return out


def _coerce_children(data: Dict[str, Any]) -> Dict[str, List[str]]:
    """Return the workflow's adjacency list, migrating from the legacy
    ``edges`` block on the fly so older JSON files keep loading.

    Legacy shapes accepted (in priority order):
    - ``{"next": "x"}`` -> ``["x"]`` (linear)
    - ``{"next": null}`` -> ``[]`` (terminal)
    - ``{"true": "a", "false": "b"}`` -> ``["a", "b"]`` (condition, true first)
    """
    if "children" in data and data["children"] is not None:
        raw = data["children"]
        out: Dict[str, List[str]] = {}
        for parent, value in raw.items():
            if value is None:
                out[parent] = []
            elif isinstance(value, list):
                out[parent] = [str(v) for v in value if v is not None]
            else:
                raise ValueError(
                    f"children[{parent!r}] must be a list, got {type(value).__name__}"
                )
        return out

    legacy = data.get("edges") or {}
    migrated: Dict[str, List[str]] = {}
    for parent, value in legacy.items():
        if value is None:
            migrated[parent] = []
        elif isinstance(value, dict):
            if "next" in value:
                nxt = value["next"]
                migrated[parent] = [] if nxt is None else [str(nxt)]
            elif "true" in value or "false" in value:
                ordered: List[str] = []
                for label in ("true", "false"):
                    child = value.get(label)
                    if child is not None:
                        ordered.append(str(child))
                for label, child in value.items():
                    if label in ("true", "false") or child is None:
                        continue
                    if str(child) not in ordered:
                        ordered.append(str(child))
                migrated[parent] = ordered
            else:
                migrated[parent] = [
                    str(v) for v in value.values() if v is not None
                ]
        elif isinstance(value, list):
            migrated[parent] = [str(v) for v in value if v is not None]
        else:
            raise ValueError(
                f"edges[{parent!r}] has unsupported shape: {type(value).__name__}"
            )
    return migrated


@dataclass
class TraceEntry:
    node_id: str
    started_at: float
    finished_at: float
    result: Any = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Job:
    """A workflow execution instance stored under Db/jobs/<id>.json."""

    id: str
    workflow_id: str
    status: JobStatus = JobStatus.QUEUED
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    heartbeat_at: Optional[float] = None
    current_node: Optional[str] = None
    input: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    trace: List[TraceEntry] = field(default_factory=list)
    parent_job_id: Optional[str] = None
    child_job_ids: List[str] = field(default_factory=list)
    error: Optional[str] = None

    @staticmethod
    def new_id() -> str:
        return "job_" + uuid.uuid4().hex[:12]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        return cls(
            id=data["id"],
            workflow_id=data["workflow_id"],
            status=JobStatus(data.get("status", "queued")),
            created_at=data.get("created_at", time.time()),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            heartbeat_at=data.get("heartbeat_at"),
            current_node=data.get("current_node"),
            input=data.get("input", {}) or {},
            context=data.get("context", {}) or {},
            trace=[TraceEntry(**t) for t in data.get("trace", [])],
            parent_job_id=data.get("parent_job_id"),
            child_job_ids=list(data.get("child_job_ids", []) or []),
            error=data.get("error"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "heartbeat_at": self.heartbeat_at,
            "current_node": self.current_node,
            "input": self.input,
            "context": self.context,
            "trace": [t.to_dict() for t in self.trace],
            "parent_job_id": self.parent_job_id,
            "child_job_ids": self.child_job_ids,
            "error": self.error,
        }
