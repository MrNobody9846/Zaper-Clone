"""Condition node evaluation.

A condition node can run in one of two modes:

* **Boolean mode** (legacy): `metric + operator + value` -> ``result: bool``.
  The executor picks the ``"true"`` / ``"false"`` entry from ``node.branches``.
* **Bucket mode** (multi-way): `metric + buckets` -> ``result: str`` (the label
  of the first matching bucket). The executor uses that label as the key into
  ``node.branches``.

Bucket entries support ``min`` (inclusive) and ``max`` (exclusive). A bucket
without either is a catch-all default. Buckets are evaluated in declaration
order; first match wins.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from ..models import Node
from .base import resolve_ref

SUPPORTED_METRICS = {"char_count", "word_count", "contains_word"}
SUPPORTED_OPERATORS = {">", "<", "==", ">=", "<=", "contains"}


def run_condition(node: Node, context: Dict[str, Any]) -> Dict[str, Any]:
    metric = node.metric
    if metric not in SUPPORTED_METRICS:
        raise ValueError(
            f"Unsupported condition metric '{metric}'. "
            f"Expected one of {sorted(SUPPORTED_METRICS)}."
        )

    source = resolve_ref(node.input, context) if node.input else None
    text = _load_text(source, context)
    measured = _measure(node, metric, text)

    if node.buckets:
        label = _pick_bucket(node.buckets, measured)
        if label is None:
            raise ValueError(
                f"Condition '{node.id}': value {measured!r} did not match any bucket. "
                f"Add a catch-all bucket (without min/max) to handle the default."
            )
        return {
            "metric": metric,
            "mode": "bucket",
            "measured": measured,
            "result": label,
            "source": source,
        }

    operator = node.operator
    if operator not in SUPPORTED_OPERATORS:
        raise ValueError(
            f"Unsupported operator '{operator}'. "
            f"Expected one of {sorted(SUPPORTED_OPERATORS)}."
        )
    threshold = node.value
    result = _compare(measured, operator, threshold)
    return {
        "metric": metric,
        "mode": "boolean",
        "operator": operator,
        "value": threshold,
        "measured": measured,
        "result": bool(result),
        "source": source,
    }


def _measure(node: Node, metric: str, text: str) -> Any:
    if metric == "char_count":
        return len(text)
    if metric == "word_count":
        return len(text.split())
    if metric == "contains_word":
        if not node.word:
            raise ValueError(
                f"Condition '{node.id}' uses metric 'contains_word' but no 'word' is set."
            )
        return node.word.lower() in text.lower()
    raise ValueError(f"Unsupported metric {metric!r}")


def _pick_bucket(buckets: List[Dict[str, Any]], measured: Any) -> Optional[str]:
    """Return the label of the first bucket that matches ``measured``."""
    for bucket in buckets:
        if "label" not in bucket:
            raise ValueError(f"Bucket is missing required 'label': {bucket!r}")
        if _bucket_matches(bucket, measured):
            return str(bucket["label"])
    return None


def _bucket_matches(bucket: Dict[str, Any], measured: Any) -> bool:
    lo = bucket.get("min")
    hi = bucket.get("max")
    eq = bucket.get("eq")
    if eq is not None:
        return measured == eq
    if lo is None and hi is None:
        return True
    try:
        if lo is not None and measured < lo:
            return False
        if hi is not None and measured >= hi:
            return False
    except TypeError as exc:
        raise TypeError(
            f"Cannot bucket {measured!r} against min={lo!r} max={hi!r}: {exc}"
        ) from exc
    return True


def _load_text(source: Any, context: Dict[str, Any]) -> str:
    """Resolve `source` to the text we want to measure."""
    if source is None:
        return ""

    if isinstance(source, dict):
        if "content" in source and isinstance(source["content"], str):
            return source["content"]
        if "path" in source:
            return _read_path(source["path"])
        return ""

    if isinstance(source, str):
        path = Path(source).expanduser()
        if path.exists() and path.is_file():
            return _read_path(path)
        return source

    return str(source)


def _read_path(path: Any) -> str:
    p = Path(path).expanduser()
    return p.read_text(encoding="utf-8")


def _compare(measured: Any, operator: str, threshold: Any) -> bool:
    if operator == "contains":
        return str(threshold) in str(measured)
    try:
        if operator == "==":
            return measured == threshold
        if operator == ">":
            return measured > threshold
        if operator == "<":
            return measured < threshold
        if operator == ">=":
            return measured >= threshold
        if operator == "<=":
            return measured <= threshold
    except TypeError as exc:
        raise TypeError(
            f"Cannot compare {measured!r} {operator} {threshold!r}: {exc}"
        ) from exc
    raise ValueError(f"Unhandled operator {operator!r}")
