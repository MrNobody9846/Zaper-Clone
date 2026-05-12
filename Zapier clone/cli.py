"""`wf` CLI — manual trigger and inspection for the workflow orchestrator."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click

# Make sure `Engine` is importable when the script is invoked as
# `python cli.py ...` from the repo root.
_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from Engine.engine import Engine  # noqa: E402
from Engine.models import Job, JobStatus, Node, NodeType, TriggerType, Workflow  # noqa: E402
from Engine.store import Store  # noqa: E402


STALE_AFTER_SECONDS = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_inputs(pairs: Tuple[str, ...]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in pairs:
        if "=" not in raw:
            raise click.BadParameter(f"--input expects key=value, got {raw!r}")
        k, v = raw.split("=", 1)
        out[k.strip()] = v
    return out


def _fmt_ts(ts: Optional[float]) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _short_status(job: Job) -> str:
    if (
        job.status is JobStatus.RUNNING
        and job.heartbeat_at is not None
        and (time.time() - job.heartbeat_at) > STALE_AFTER_SECONDS
    ):
        return "running (stale)"
    return job.status.value


def _print_table(headers: List[str], rows: List[List[str]]) -> None:
    if not rows:
        click.echo("(none)")
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    fmt = "  ".join("{:<" + str(w) + "}" for w in widths)
    click.echo(fmt.format(*headers))
    click.echo(fmt.format(*["-" * w for w in widths]))
    for row in rows:
        click.echo(fmt.format(*row))


# ---------------------------------------------------------------------------
# Root command
# ---------------------------------------------------------------------------


@click.group(
    help="wf — a tiny workflow orchestrator. Manage and run JSON-defined workflows."
)
def main() -> None:  # noqa: D401 - click handles docs
    pass


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


@main.command(help="Run a workflow by id (fires its trigger node).")
@click.argument("workflow_id")
@click.option(
    "--max-instances",
    "-m",
    type=int,
    default=5,
    show_default=True,
    help="Maximum concurrent workflow instances.",
)
@click.option(
    "--input",
    "-i",
    "inputs",
    multiple=True,
    metavar="KEY=VALUE",
    help="Repeatable. Inputs forwarded to the trigger node.",
)
@click.option(
    "--detach",
    "-d",
    is_flag=True,
    help="Spawn the run in a background subprocess and return the job id immediately.",
)
@click.option(
    "--force-fire",
    is_flag=True,
    help="Bypass non-manual trigger logic (use job input as the trigger payload).",
)
def run(
    workflow_id: str,
    max_instances: int,
    inputs: Tuple[str, ...],
    detach: bool,
    force_fire: bool,
) -> None:
    store = Store()
    try:
        store.load_workflow(workflow_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    input_kv = _parse_inputs(inputs)

    if detach:
        log_dir = store.jobs_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "run",
            workflow_id,
            "--max-instances",
            str(max_instances),
        ]
        if force_fire:
            cmd.append("--force-fire")
        for k, v in input_kv.items():
            cmd.extend(["--input", f"{k}={v}"])
        log_path = log_dir / "_detached.log"
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"\n=== {datetime.now().isoformat()} {' '.join(cmd)} ===\n")
            proc = subprocess.Popen(
                cmd,
                stdout=fh,
                stderr=subprocess.STDOUT,
                cwd=str(_REPO_ROOT),
                start_new_session=True,
            )
        click.echo(f"detached pid={proc.pid} log={log_path}")
        return

    with Engine(max_instances=max_instances, store=store, force_fire=force_fire) as engine:
        job_id = engine.submit(workflow_id, input=input_kv)
        click.echo(f"submitted job {job_id} (workflow={workflow_id})")
        engine.wait_all()

    final = store.load_job(job_id)
    click.echo(f"job {job_id} -> {final.status.value}")
    if final.child_job_ids:
        click.echo(f"  children: {', '.join(final.child_job_ids)}")
        for child_id in final.child_job_ids:
            try:
                child = store.load_job(child_id)
                click.echo(f"    {child.id} -> {child.status.value}")
            except FileNotFoundError:
                pass
    if final.error:
        click.echo(f"  error: {final.error}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@main.command(name="list", help="List jobs (default), workflows, or nodes.")
@click.argument(
    "kind",
    required=False,
    default="jobs",
    type=click.Choice(["jobs", "workflows", "nodes"]),
)
@click.option(
    "--status",
    "-s",
    type=click.Choice([s.value for s in JobStatus]),
    help="Filter jobs by status.",
)
@click.option(
    "--workflow",
    "-w",
    "workflow_filter",
    help="Filter jobs to a specific workflow id.",
)
@click.option(
    "--limit", "-n", type=int, default=20, show_default=True, help="Max rows."
)
def list_(
    kind: str, status: Optional[str], workflow_filter: Optional[str], limit: int
) -> None:
    store = Store()
    if kind == "jobs":
        jobs = store.list_jobs()
        if status:
            jobs = [j for j in jobs if j.status.value == status]
        if workflow_filter:
            jobs = [j for j in jobs if j.workflow_id == workflow_filter]
        jobs = jobs[:limit]
        rows = [
            [
                j.id,
                j.workflow_id,
                _short_status(j),
                j.current_node or "-",
                _fmt_ts(j.started_at or j.created_at),
                "child" if j.parent_job_id else "root",
            ]
            for j in jobs
        ]
        _print_table(
            ["JOB_ID", "WORKFLOW", "STATUS", "NODE", "STARTED", "ROLE"], rows
        )
        return

    if kind == "workflows":
        wfs = store.list_workflows()[:limit]
        rows = [[w.id, w.name, w.start, str(len(w.children))] for w in wfs]
        _print_table(["ID", "NAME", "START", "NODES"], rows)
        return

    if kind == "nodes":
        nodes = store.list_nodes()[:limit]
        rows = []
        for n in nodes:
            detail = ""
            if n.type is NodeType.ACTION:
                detail = f"action={n.action}"
            elif n.type is NodeType.TRIGGER:
                detail = f"trigger_type={n.trigger_type.value if n.trigger_type else '?'}"
            elif n.type is NodeType.CONDITION:
                detail = f"{n.metric} {n.operator} {n.value!r}"
            rows.append([n.id, n.type.value, detail])
        _print_table(["ID", "TYPE", "DETAIL"], rows)
        return


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@main.command(help="Show status + trace for a job id, or a summary for a workflow id.")
@click.argument("identifier")
def status(identifier: str) -> None:
    store = Store()

    job: Optional[Job] = None
    try:
        job = store.load_job(identifier)
    except FileNotFoundError:
        job = None

    if job is not None:
        _print_job(job, store)
        return

    try:
        wf = store.load_workflow(identifier)
    except FileNotFoundError:
        raise click.ClickException(
            f"No job or workflow with id {identifier!r}."
        )
    _print_workflow_summary(wf, store)


# ---------------------------------------------------------------------------
# graph
# ---------------------------------------------------------------------------


@main.command(help="Print the workflow's adjacency map (parent -> [children]).")
@click.argument("workflow_id")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit JSON suitable for piping into `jq` or graph tools.",
)
def graph(workflow_id: str, as_json: bool) -> None:
    store = Store()
    try:
        wf = store.load_workflow(workflow_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(
            json.dumps(
                {
                    "id": wf.id,
                    "start": wf.start,
                    "children": wf.children,
                    "fanout": wf.fanout,
                },
                indent=2,
            )
        )
        return

    click.echo(f"{wf.id}  (start = {wf.start})")
    if not wf.children:
        click.echo("  (no children map)")
        return

    width = max(len(p) for p in wf.children)
    for parent, kids in wf.children.items():
        suffix = ""
        try:
            node = store.load_node(parent)
        except FileNotFoundError:
            node = None
        if node and node.type is NodeType.CONDITION and node.branches:
            labelled = ", ".join(f"{k}={v}" for k, v in node.branches.items())
            suffix = f"   (condition: {labelled})"
        rendered = "[" + ", ".join(kids) + "]" if kids else "[]"
        terminal = "   (terminal)" if not kids else ""
        click.echo(f"  {parent.ljust(width)}  ->  {rendered}{terminal}{suffix}")

    if wf.fanout:
        f = wf.fanout
        click.echo(
            f"fanout: {f.get('node')}.{f.get('over')} -> "
            f"child_start={f.get('child_start')} as={f.get('as')}"
        )


def _print_job(job: Job, store: Store) -> None:
    click.echo(f"job:        {job.id}")
    click.echo(f"workflow:   {job.workflow_id}")
    click.echo(f"status:     {_short_status(job)}")
    click.echo(f"created:    {_fmt_ts(job.created_at)}")
    click.echo(f"started:    {_fmt_ts(job.started_at)}")
    click.echo(f"finished:   {_fmt_ts(job.finished_at)}")
    click.echo(f"heartbeat:  {_fmt_ts(job.heartbeat_at)}")
    if job.current_node:
        click.echo(f"node:       {job.current_node}")
    if job.parent_job_id:
        click.echo(f"parent:     {job.parent_job_id}")
    if job.child_job_ids:
        click.echo(f"children:   {', '.join(job.child_job_ids)}")
    if job.input:
        click.echo(f"input:      {json.dumps(job.input)}")
    if job.error:
        click.echo("error:")
        for line in job.error.splitlines():
            click.echo(f"   {line}")
    if job.trace:
        click.echo("trace:")
        for entry in job.trace:
            dur = (entry.finished_at - entry.started_at) * 1000.0
            if entry.error:
                click.echo(
                    f"  - {entry.node_id:20}  {dur:7.1f} ms  ERROR  {entry.error}"
                )
            else:
                preview = _preview(entry.result)
                click.echo(
                    f"  - {entry.node_id:20}  {dur:7.1f} ms  ok     {preview}"
                )


def _print_workflow_summary(wf: Workflow, store: Store) -> None:
    jobs = list(store.iter_jobs_for_workflow(wf.id))
    counts: Dict[str, int] = {s.value: 0 for s in JobStatus}
    for j in jobs:
        counts[j.status.value] = counts.get(j.status.value, 0) + 1
    click.echo(f"workflow:  {wf.id}  ({wf.name})")
    click.echo(f"start:     {wf.start}")
    click.echo(f"runs:      {len(jobs)} total")
    for status_name in ("queued", "running", "completed", "failed"):
        click.echo(f"  {status_name:<10} {counts.get(status_name, 0)}")
    click.echo("recent:")
    for j in jobs[:10]:
        click.echo(
            f"  {j.id}  {_short_status(j):16}  started={_fmt_ts(j.started_at or j.created_at)}"
        )


def _preview(value, limit: int = 80) -> str:
    try:
        s = json.dumps(value, default=str)
    except (TypeError, ValueError):
        s = str(value)
    if len(s) > limit:
        s = s[: limit - 3] + "..."
    return s


# ---------------------------------------------------------------------------
# init-samples
# ---------------------------------------------------------------------------


@main.command(name="init-samples", help="Write a small example workflow + nodes into Db/.")
@click.option("--force", is_flag=True, help="Overwrite existing sample files.")
def init_samples(force: bool) -> None:
    store = Store()

    samples = _sample_definitions()
    for node in samples["nodes"]:
        target = store.nodes_dir / f"{node.id}.json"
        if target.exists() and not force:
            click.echo(f"skip   node     {node.id} (exists)")
            continue
        store.save_node(node)
        click.echo(f"wrote  node     {node.id}")

    for wf in samples["workflows"]:
        target = store.workflows_dir / f"{wf.id}.json"
        if target.exists() and not force:
            click.echo(f"skip   workflow {wf.id} (exists)")
            continue
        store.save_workflow(wf)
        click.echo(f"wrote  workflow {wf.id}")

    sample_input = _REPO_ROOT / "input"
    sample_input.mkdir(exist_ok=True)
    small = sample_input / "small.txt"
    big = sample_input / "big.txt"
    if not small.exists():
        small.write_text("hello there\n", encoding="utf-8")
        click.echo(f"wrote  input    {small.name} (small sample)")
    if not big.exists():
        big.write_text(("the quick brown fox " * 30) + "\n", encoding="utf-8")
        click.echo(f"wrote  input    {big.name} (big sample)")

    click.echo("")
    click.echo("Try: wf run wf_demo --max-instances 3")


def _sample_definitions():
    output = _REPO_ROOT / "output"
    nodes = [
        Node(id="manual_start", type=NodeType.TRIGGER, trigger_type=TriggerType.MANUAL),
        Node(
            id="list_input",
            type=NodeType.ACTION,
            action="list_files",
            params={"dir": str(_REPO_ROOT / "input"), "pattern": "*.txt"},
        ),
        Node(
            id="is_big",
            type=NodeType.CONDITION,
            metric="word_count",
            input="$current_file",
            operator=">",
            value=20,
            branches={"true": "write_big", "false": "write_small"},
        ),
        Node(
            id="write_big",
            type=NodeType.ACTION,
            action="write_file",
            params={
                "path": str(output / "big_files.log"),
                "content": "BIG: $current_file\n",
                "append": True,
            },
        ),
        Node(
            id="write_small",
            type=NodeType.ACTION,
            action="write_file",
            params={
                "path": str(output / "small_files.log"),
                "content": "small: $current_file\n",
                "append": True,
            },
        ),
        # ---- wf_buckets: 4-way multi-branch condition ----
        Node(
            id="size_bucket",
            type=NodeType.CONDITION,
            metric="word_count",
            input="$current_file",
            buckets=[
                {"label": "tiny", "max": 10},
                {"label": "small", "max": 50},
                {"label": "medium", "max": 200},
                {"label": "large"},
            ],
            branches={
                "tiny": "write_tiny",
                "small": "write_small_bkt",
                "medium": "write_medium",
                "large": "write_large",
            },
        ),
        Node(
            id="write_tiny",
            type=NodeType.ACTION,
            action="write_file",
            params={
                "path": str(output / "tiny.log"),
                "content": "tiny:   $current_file\n",
                "append": True,
            },
        ),
        Node(
            id="write_small_bkt",
            type=NodeType.ACTION,
            action="write_file",
            params={
                "path": str(output / "small.log"),
                "content": "small:  $current_file\n",
                "append": True,
            },
        ),
        Node(
            id="write_medium",
            type=NodeType.ACTION,
            action="write_file",
            params={
                "path": str(output / "medium.log"),
                "content": "medium: $current_file\n",
                "append": True,
            },
        ),
        Node(
            id="write_large",
            type=NodeType.ACTION,
            action="write_file",
            params={
                "path": str(output / "large.log"),
                "content": "large:  $current_file\n",
                "append": True,
            },
        ),
    ]
    wf_demo = Workflow(
        id="wf_demo",
        name="Sort input files by word count (boolean)",
        start="manual_start",
        children={
            "manual_start": ["list_input"],
            "list_input": ["is_big"],
            "is_big": ["write_big", "write_small"],
            "write_big": [],
            "write_small": [],
        },
        fanout={
            "node": "list_input",
            "over": "files",
            "as": "current_file",
            "child_start": "is_big",
        },
    )
    wf_buckets = Workflow(
        id="wf_buckets",
        name="Sort input files into tiny / small / medium / large (4-way)",
        start="manual_start",
        children={
            "manual_start": ["list_input"],
            "list_input": ["size_bucket"],
            "size_bucket": [
                "write_tiny",
                "write_small_bkt",
                "write_medium",
                "write_large",
            ],
            "write_tiny": [],
            "write_small_bkt": [],
            "write_medium": [],
            "write_large": [],
        },
        fanout={
            "node": "list_input",
            "over": "files",
            "as": "current_file",
            "child_start": "size_bucket",
        },
    )
    return {"nodes": nodes, "workflows": [wf_demo, wf_buckets]}


if __name__ == "__main__":
    main()
