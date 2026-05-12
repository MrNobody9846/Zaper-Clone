"""Action nodes: write_file, read_file, move_file, list_files."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict

from ..models import Node
from .base import resolve_params

ACTION_NAMES = {"write_file", "read_file", "move_file", "list_files"}


def run_action(node: Node, context: Dict[str, Any]) -> Dict[str, Any]:
    if node.action not in ACTION_NAMES:
        raise ValueError(
            f"Unknown action '{node.action}'. Expected one of {sorted(ACTION_NAMES)}."
        )
    params = resolve_params(node.params or {}, context)
    handler = _HANDLERS[node.action]
    return handler(params)


def _write_file(params: Dict[str, Any]) -> Dict[str, Any]:
    path = Path(_require(params, "path")).expanduser()
    content = params.get("content", "")
    if not isinstance(content, str):
        content = str(content)
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if params.get("append") else "w"
    with path.open(mode, encoding="utf-8") as fh:
        fh.write(content)
    return {"path": str(path), "bytes_written": len(content.encode("utf-8"))}


def _read_file(params: Dict[str, Any]) -> Dict[str, Any]:
    path = Path(_require(params, "path")).expanduser()
    text = path.read_text(encoding="utf-8")
    return {
        "path": str(path),
        "content": text,
        "char_count": len(text),
        "word_count": len(text.split()),
    }


def _move_file(params: Dict[str, Any]) -> Dict[str, Any]:
    src = Path(_require(params, "src")).expanduser()
    dest_dir = Path(_require(params, "dest_dir")).expanduser()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.move(str(src), str(dest))
    return {"src": str(src), "dest": str(dest)}


def _list_files(params: Dict[str, Any]) -> Dict[str, Any]:
    directory = Path(_require(params, "dir")).expanduser()
    pattern = params.get("pattern", "*")
    if not directory.exists() or not directory.is_dir():
        return {"dir": str(directory), "files": [], "count": 0}
    files = sorted(str(p) for p in directory.glob(pattern) if p.is_file())
    return {"dir": str(directory), "files": files, "count": len(files)}


def _require(params: Dict[str, Any], key: str) -> Any:
    if key not in params or params[key] is None:
        raise KeyError(f"Action is missing required param '{key}'")
    return params[key]


_HANDLERS = {
    "write_file": _write_file,
    "read_file": _read_file,
    "move_file": _move_file,
    "list_files": _list_files,
}
