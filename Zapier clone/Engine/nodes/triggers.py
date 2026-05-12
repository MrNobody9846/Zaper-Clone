"""Trigger nodes.

In this MVP only the `manual` trigger is wired end-to-end. The other
trigger types are parsed and recognised so workflows can declare them, but
they raise `NotImplementedError` until the daemon that fires them is built.

# TODO(triggers): implement schedule/file_watch/webhook trigger runtime
# (likely a `wf daemon` command that watches enabled triggers and calls
# engine.submit() when they fire).
"""

from __future__ import annotations

from typing import Any, Dict

from ..models import Node, TriggerType


def run_trigger(node: Node, context: Dict[str, Any], *, force_fire: bool = False) -> Dict[str, Any]:
    """Fire a trigger node and return the data it produces.

    For `manual`, this returns whatever payload was supplied as job input.
    Other trigger types raise `NotImplementedError` unless `force_fire=True`.
    """
    tt = node.trigger_type
    if tt is None:
        raise ValueError(f"Trigger node '{node.id}' is missing trigger_type")

    if tt is TriggerType.MANUAL:
        return dict(context.get("__job_input__", {}))

    if force_fire:
        return dict(context.get("__job_input__", {}))

    raise NotImplementedError(
        f"Trigger type '{tt.value}' is not yet supported. Re-run with --force-fire "
        f"to bypass and use the job input as the trigger payload."
    )
