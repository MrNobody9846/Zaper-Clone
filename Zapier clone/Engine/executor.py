"""Execute a single workflow instance.

The executor walks the graph from the workflow's `start` node, recording each
step in `job.trace`. Trigger and action nodes follow their `next` edge;
condition nodes follow `true`/`false` based on the evaluation result.

If the workflow declares `fanout`, then after the named source node runs, one
child job is submitted per item in `over` (via `engine.submit`) and the parent
job exits. Children are independent jobs that themselves enqueue if the
engine is at max-instances.
"""

from __future__ import annotations

import time
import traceback
from typing import TYPE_CHECKING, Any, Dict, Optional

from .models import Job, JobStatus, Node, NodeType, TraceEntry, Workflow
from .nodes.actions import run_action
from .nodes.conditions import run_condition
from .nodes.triggers import run_trigger
from .store import Store

if TYPE_CHECKING:  # avoid runtime circular import
    from .engine import Engine


class WorkflowExecutor:
    def __init__(self, engine: "Engine", store: Store) -> None:
        self.engine = engine
        self.store = store

    def run(self, job: Job, *, force_fire: bool = False) -> None:
        job.status = JobStatus.RUNNING
        job.started_at = time.time()
        job.heartbeat_at = job.started_at
        self.store.save_job(job)

        try:
            workflow = self.store.load_workflow(job.workflow_id)
        except Exception as exc:
            self._fail(job, f"Failed to load workflow {job.workflow_id}: {exc}")
            return

        try:
            self._validate_condition_branches(workflow)
        except Exception as exc:
            self._fail(job, f"{type(exc).__name__}: {exc}")
            return

        context: Dict[str, Any] = dict(job.context)
        context.setdefault("__job_input__", dict(job.input))

        if "current_file" in job.input and "current_file" not in context:
            context["current_file"] = job.input["current_file"]

        # Honour an executor-supplied start override (used by fanout children).
        start = job.input.get("__override_start__") or workflow.start
        current: Optional[str] = start
        fanout_cfg = workflow.fanout
        fanout_done = False

        try:
            while current is not None:
                node = self.store.load_node(current)
                job.current_node = current
                job.heartbeat_at = time.time()
                self.store.save_job(job)

                started = time.time()
                try:
                    result = self._execute_node(node, context, force_fire=force_fire)
                except Exception as exc:
                    job.trace.append(
                        TraceEntry(
                            node_id=node.id,
                            started_at=started,
                            finished_at=time.time(),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    raise

                context[node.id] = result
                job.trace.append(
                    TraceEntry(
                        node_id=node.id,
                        started_at=started,
                        finished_at=time.time(),
                        result=_safe_for_json(result),
                    )
                )
                job.context = _safe_for_json(context)
                self.store.save_job(job)

                if (
                    fanout_cfg
                    and not fanout_done
                    and fanout_cfg.get("node") == node.id
                    and job.parent_job_id is None
                ):
                    self._fanout(job, fanout_cfg, result)
                    fanout_done = True
                    current = None
                    break

                current = self._next_node(workflow, node, result)

        except Exception as exc:
            self._fail(
                job,
                f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            )
            return

        job.status = JobStatus.COMPLETED
        job.finished_at = time.time()
        job.heartbeat_at = job.finished_at
        job.current_node = None
        self.store.save_job(job)

    def _execute_node(
        self, node: Node, context: Dict[str, Any], *, force_fire: bool
    ) -> Any:
        if node.type is NodeType.TRIGGER:
            return run_trigger(node, context, force_fire=force_fire)
        if node.type is NodeType.ACTION:
            return run_action(node, context)
        if node.type is NodeType.CONDITION:
            return run_condition(node, context)
        raise ValueError(f"Unknown node type: {node.type!r}")

    def _next_node(
        self, workflow: Workflow, node: Node, result: Any
    ) -> Optional[str]:
        kids = workflow.children.get(node.id, [])
        if node.type is NodeType.CONDITION:
            if not node.branches:
                raise ValueError(
                    f"Condition node '{node.id}' has no `branches` map; "
                    f"add a `branches` map mapping each possible result label to a child node id."
                )
            branch = _branch_key(result.get("result") if isinstance(result, dict) else result)
            chosen = node.branches.get(branch)
            if chosen is None:
                raise ValueError(
                    f"Condition node '{node.id}' is missing the {branch!r} branch "
                    f"(available: {sorted(node.branches)})."
                )
            if chosen not in kids:
                raise ValueError(
                    f"Condition node '{node.id}' routes {branch!r} -> {chosen!r}, "
                    f"but {chosen!r} is not in children[{node.id!r}] = {kids}."
                )
            return chosen
        return kids[0] if kids else None

    def _validate_condition_branches(self, workflow: Workflow) -> None:
        """Pre-flight: every condition node referenced in `children` must have
        a `branches` map whose values all live in that node's children list."""
        for parent_id, kids in workflow.children.items():
            try:
                node = self.store.load_node(parent_id)
            except FileNotFoundError:
                continue
            if node.type is not NodeType.CONDITION:
                continue
            if not node.branches:
                raise ValueError(
                    f"Condition node '{parent_id}' has no `branches` map."
                )
            for label, child in node.branches.items():
                if child not in kids:
                    raise ValueError(
                        f"Condition node '{parent_id}': branch {label!r} -> {child!r} "
                        f"is not present in children[{parent_id!r}] = {kids}."
                    )

    def _fanout(self, job: Job, cfg: Dict[str, Any], result: Any) -> None:
        over_field = cfg.get("over")
        items = result.get(over_field) if isinstance(result, dict) else None
        if not isinstance(items, list):
            raise ValueError(
                f"fanout: expected list under '{over_field}' from node "
                f"'{cfg.get('node')}', got {type(items).__name__}"
            )
        child_start = cfg.get("child_start")
        item_name = cfg.get("as", "current_file")
        child_ids = []
        for item in items:
            child_input = dict(job.input)
            child_input[item_name] = item
            child_id = self.engine.submit(
                workflow_id=job.workflow_id,
                input=child_input,
                parent_job_id=job.id,
                override_start=child_start,
            )
            child_ids.append(child_id)
        job.child_job_ids.extend(child_ids)
        self.store.save_job(job)

    def _fail(self, job: Job, error: str) -> None:
        job.status = JobStatus.FAILED
        job.error = error
        job.finished_at = time.time()
        job.heartbeat_at = job.finished_at
        self.store.save_job(job)


def _branch_key(result: Any) -> str:
    """Derive the `Node.branches` lookup key from a condition's result value.

    * `bool` -> ``"true"`` / ``"false"`` (boolean conditions).
    * `str`  -> the string itself (bucket / switch conditions).
    * Anything else -> ``str(...)`` as a best effort so misconfigured
      conditions surface a clear "missing branch" error in `_next_node`.
    """
    if isinstance(result, bool):
        return "true" if result else "false"
    if isinstance(result, str):
        return result
    return str(result)


def _safe_for_json(value: Any) -> Any:
    """Best-effort coercion so the job context round-trips through json.dump."""
    if isinstance(value, dict):
        return {str(k): _safe_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_safe_for_json(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
