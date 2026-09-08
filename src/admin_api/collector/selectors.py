"""Match stored nodes against a resource configuration's node selectors.

A `Selector` names a `Node` attribute (`type`) and the value it must hold
(`value`). Selectors that name different attributes are AND-ed; selectors that
repeat an attribute are OR-ed within it. Comparison is case-insensitive and
stringised, so `{"type": "port", "value": "161"}` works too. A selector naming
an attribute the `Node` model does not have matches nothing.
"""

from collections import defaultdict

from admin_api.nodes.model import Node

from .model import Selector


def node_matches(node: Node, selectors: list[Selector]) -> bool:
    """Whether `node` satisfies every selector. No selectors means "match all"."""
    if not selectors:
        return True

    wanted: dict[str, set[str]] = defaultdict(set)
    for selector in selectors:
        wanted[selector.type].add(selector.value.casefold())

    for attr, values in wanted.items():
        actual = getattr(node, attr, None)
        if actual is None or str(actual).casefold() not in values:
            return False
    return True


def select_nodes(nodes: list[Node], selectors: list[Selector]) -> list[Node]:
    """Return the nodes that satisfy `selectors`, preserving input order."""
    return [node for node in nodes if node_matches(node, selectors)]
