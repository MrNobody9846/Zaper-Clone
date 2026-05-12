"""Shared node interface plus context-reference resolution."""

from __future__ import annotations

import re
from typing import Any, Dict, Protocol

from ..models import Node


class NodeRunner(Protocol):
    def execute(self, node: Node, context: Dict[str, Any]) -> Any:  # pragma: no cover
        ...


_REF_RE = re.compile(r"^\$(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?P<rest>.*)$")
_INDEX_RE = re.compile(r"\[(\d+)\]")
# A token usable inside a larger string: `$name`, `$name.field`, `$name.field[0]`.
_TOKEN_RE = re.compile(
    r"\$(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?P<rest>(?:\.[A-Za-z_][A-Za-z0-9_]*|\[\d+\])*)"
)


def _walk(cursor: Any, rest: str, original: str) -> Any:
    """Walk a `.field[0].sub` accessor path off a starting value."""
    while rest:
        if rest.startswith("."):
            rest = rest[1:]
            dot = rest.find(".")
            bracket = rest.find("[")
            ends = [x for x in (dot, bracket) if x != -1]
            end = min(ends) if ends else len(rest)
            key = rest[:end]
            rest = rest[end:]
            if not isinstance(cursor, dict) or key not in cursor:
                raise KeyError(
                    f"Cannot resolve '.{key}' on {type(cursor).__name__} for ref {original}"
                )
            cursor = cursor[key]
        elif rest.startswith("["):
            m2 = _INDEX_RE.match(rest)
            if not m2:
                raise ValueError(f"Bad index in ref {original!r}")
            idx = int(m2.group(1))
            rest = rest[m2.end():]
            cursor = cursor[idx]
        else:
            break
    return cursor


def resolve_ref(value: Any, context: Dict[str, Any]) -> Any:
    """Resolve a reference, with two modes depending on the input.

    - If `value` is a string that is *entirely* one reference (e.g.
      `"$list_input.files[0]"`), return the raw referenced object (could be a
      dict/list).
    - If `value` is a string containing `$name` tokens mixed with other text
      (e.g. `"BIG: $current_file\n"`), interpolate: each token is replaced by
      `str(...)` of the referenced value.
    - Otherwise (non-string, or string without any `$`), return as-is.

    Missing context keys raise `KeyError` so authoring mistakes surface loudly.
    """
    if not isinstance(value, str) or "$" not in value:
        return value

    m = _REF_RE.fullmatch(value)
    if m:
        name = m.group("name")
        if name not in context:
            raise KeyError(
                f"No context entry named '{name}' (full ref: {value})"
            )
        return _walk(context[name], m.group("rest"), value)

    def _sub(match: "re.Match[str]") -> str:
        name = match.group("name")
        if name not in context:
            raise KeyError(
                f"No context entry named '{name}' (in template: {value!r})"
            )
        resolved = _walk(context[name], match.group("rest"), match.group(0))
        return str(resolved)

    return _TOKEN_RE.sub(_sub, value)


def resolve_params(params: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-resolve `$ref` strings inside a params dict."""
    out: Dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(v, dict):
            out[k] = resolve_params(v, context)
        elif isinstance(v, list):
            out[k] = [resolve_ref(item, context) for item in v]
        else:
            out[k] = resolve_ref(v, context)
    return out
