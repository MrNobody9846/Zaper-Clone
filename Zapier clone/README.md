# Workflow Orchestrator (`wf`)

A tiny, file-driven workflow orchestrator written in Python. Workflows are JSON
files that wire together three kinds of nodes — **trigger**, **action**,
**condition** — and run inside an engine that can execute many instances in
parallel with a configurable cap.

There is no database: definitions and run records all live as JSON under `Db/`.

## Layout

```
.
├── cli.py                # Click-based CLI (also exposed as `wf`)
├── wf                    # Thin shell wrapper for ./wf without installing
├── pyproject.toml        # Installs the `wf` console-script
├── requirements.txt
├── Engine/
│   ├── engine.py         # ThreadPool orchestrator with max_instances + queue
│   ├── executor.py       # Walks a workflow graph for one job
│   ├── models.py         # Node / Workflow / Job dataclasses
│   ├── store.py          # JSON read/write for Db/
│   └── nodes/
│       ├── base.py
│       ├── triggers.py   # manual (wired) + schedule/file_watch/webhook stubs
│       ├── actions.py    # write_file / read_file / move_file / list_files
│       └── conditions.py # char_count / word_count / contains_word
└── Db/
    ├── nodes/            # reusable node definitions, one JSON each
    ├── workflows/        # workflow graphs, one JSON each
    └── jobs/             # one JSON per execution instance (status + trace)
```

## Quickstart

Requires Python 3.9+.

### 1. Create a virtual environment and install deps

```bash
# from the project root
python3 -m venv .venv
source .venv/bin/activate              # macOS / Linux
# .venv\Scripts\activate              # Windows PowerShell

pip install --upgrade pip
pip install -r requirements.txt
```

Optional — install the project so the `wf` command is on your PATH globally
(still uses the active venv):

```bash
pip install -e .
```

The bundled `./wf` shell wrapper auto-detects `.venv/bin/python` if present,
so you can also just skip activation and call `./wf` directly after the
`pip install -r requirements.txt` step.

### 2. Run the demos

```bash
./wf init-samples
./wf list workflows
./wf run wf_demo --max-instances 3       # boolean condition (true/false)
./wf run wf_buckets --max-instances 3    # 4-way bucket condition
./wf list
./wf status <job_id>
./wf graph wf_buckets
```

### Deactivating / cleaning up

```bash
deactivate                              # leave the venv shell
rm -rf .venv                            # delete the env entirely
```

## Node types

### Trigger
Every workflow's `start` must reference a `trigger` node.

```json
{ "id": "manual_start", "type": "trigger", "trigger_type": "manual" }
```

Only `manual` is wired in this MVP. `schedule`, `file_watch`, and `webhook` are
parsed but raise `NotImplementedError` at runtime. Pass `--force-fire` to `wf
run` to bypass them for testing.

### Action
```json
{ "id": "list_input", "type": "action", "action": "list_files", "params": { "dir": "./input" } }
```
Actions: `write_file`, `read_file`, `move_file`, `list_files`.

### Condition
Conditions have two modes.

**Boolean mode** — produces `true` / `false`:
```json
{ "id": "is_big", "type": "condition",
  "metric": "word_count",
  "input": "$current_file",
  "operator": ">", "value": 100,
  "branches": { "true": "write_big", "false": "write_small" } }
```
Metrics: `char_count`, `word_count`, `contains_word` (with `word` param).
Operators: `>`, `<`, `==`, `>=`, `<=`, `contains`.

**Bucket mode** — N-way routing on a numeric metric. Add a `buckets` list
instead of `operator`/`value`; the condition emits the **label** of the first
matching bucket, and the executor uses that label as the key into `branches`:
```json
{ "id": "size_bucket", "type": "condition",
  "metric": "word_count",
  "input": "$current_file",
  "buckets": [
    { "label": "tiny",   "max": 10 },
    { "label": "small",  "max": 50 },
    { "label": "medium", "max": 200 },
    { "label": "large" }
  ],
  "branches": {
    "tiny":   "write_tiny",
    "small":  "write_small",
    "medium": "write_medium",
    "large":  "write_large"
  }
}
```
Each bucket may declare `min` (inclusive), `max` (exclusive), or `eq`. A bucket
with no predicate is the catch-all default. Buckets are evaluated in
declaration order; first match wins.

`branches` tells the executor which child to follow for each result label. Every
value here must also appear in the workflow's `children[<this_node>]` list —
that's checked when the workflow loads.

## Workflow schema

The graph is a pure adjacency list: `children` maps a parent node id to the
list of its child node ids. An empty list marks a terminal node. Condition
routing lives on the condition node itself (`branches` above), so this map
stays a clean `Dict[str, List[str]]` you can drop into any graph algorithm
(BFS, DFS, topological sort, cycle detection).

```json
{
  "id": "wf_demo",
  "name": "Process files by size",
  "start": "manual_start",
  "children": {
    "manual_start": ["list_input"],
    "list_input":   ["is_big"],
    "is_big":       ["write_big", "write_small"],
    "write_big":    [],
    "write_small":  []
  },
  "fanout": { "node": "list_input", "over": "files", "as": "current_file", "child_start": "is_big" }
}
```

`fanout` (optional): after the listed node, the engine spawns one child job per
item in its `over` field, starting at `child_start`. Each item is exposed in
the child's context as the name in `as`.

Node outputs are referenced via `$node_id.field` strings, resolved at runtime
from the job's context (e.g. `$list_input.files[0]`). Inside a fanout child
the item is also available as `$<as>` (e.g. `$current_file`).

Legacy workflow JSONs that still use the old `edges` block are auto-migrated
on read (`{"next": x}` -> `[x]`, condition `{"true": a, "false": b}` -> `[a, b]`).
Run `wf init-samples --force` to rewrite them in the new shape.

## CLI reference

```
wf run <workflow_id> [--max-instances N] [--input k=v ...] [--detach] [--force-fire]
wf list                                # jobs view (running workflows), newest first
wf list (workflows|jobs|nodes)
wf list --status (queued|running|completed|failed) [--limit N]
wf status <job_id_or_workflow_id>
wf graph <workflow_id> [--json]        # print the children adjacency map
wf init-samples [--force]
```

Example `wf graph wf_demo`:

```
wf_demo  (start = manual_start)
  manual_start  ->  [list_input]
  list_input    ->  [is_big]
  is_big        ->  [write_big, write_small]   (condition: true=write_big, false=write_small)
  write_big     ->  []   (terminal)
  write_small   ->  []   (terminal)
fanout: list_input.files -> child_start=is_big as=current_file
```

`wf graph <id> --json` emits `{"id", "start", "children", "fanout"}` ready to pipe into `jq` or any graph tool.

Status updates are flushed to `Db/jobs/<id>.json` after every node, so a second
terminal running `wf list` / `wf status` sees the live state. A `heartbeat_at`
field is updated each step; running rows older than 60 s are flagged
`(stale)` so crashed runs don't appear as running forever.

## Roadmap

- Wire `schedule`, `file_watch`, `webhook` triggers behind `wf daemon`.
- Swap `Engine._run_job` for `docker run …` to graduate to one container per
  workflow instance.
