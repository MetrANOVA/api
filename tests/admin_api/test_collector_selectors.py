from admin_api.collector.model import Selector
from admin_api.collector.selectors import node_matches, select_nodes
from admin_api.nodes.model import Node


def _node(**overrides) -> Node:
    defaults = dict(
        node_id="n1",
        host="10.0.0.1",
        port=161,
        community="public",
        name="core-rtr-1",
        make="Cisco",
        model="ASR9000",
    )
    defaults.update(overrides)
    return Node(**defaults)


def _sel(type_, value) -> Selector:
    return Selector(type=type_, value=value)


def test_no_selectors_matches_every_node():
    assert node_matches(_node(), []) is True


def test_single_selector_exact_match():
    assert node_matches(_node(make="Cisco"), [_sel("make", "Cisco")]) is True
    assert node_matches(_node(make="Juniper"), [_sel("make", "Cisco")]) is False


def test_match_is_case_insensitive():
    assert node_matches(_node(make="Cisco"), [_sel("make", "cisco")]) is True


def test_selectors_on_different_fields_are_anded():
    node = _node(make="Cisco", model="ASR9000")
    assert node_matches(node, [_sel("make", "cisco"), _sel("model", "asr9000")]) is True
    assert node_matches(node, [_sel("make", "cisco"), _sel("model", "mx960")]) is False


def test_repeated_field_is_ored():
    selectors = [_sel("make", "cisco"), _sel("make", "juniper")]
    assert node_matches(_node(make="Juniper"), selectors) is True
    assert node_matches(_node(make="Arista"), selectors) is False


def test_unknown_field_matches_nothing():
    assert node_matches(_node(), [_sel("role", "rtr")]) is False


def test_non_string_attribute_is_stringified():
    assert node_matches(_node(port=161), [_sel("port", "161")]) is True
    assert node_matches(_node(port=161), [_sel("port", "162")]) is False


def test_select_nodes_filters_and_preserves_order():
    nodes = [
        _node(node_id="a", make="Cisco"),
        _node(node_id="b", make="Juniper"),
        _node(node_id="c", make="Cisco"),
    ]

    selected = select_nodes(nodes, [_sel("make", "cisco")])

    assert [n.node_id for n in selected] == ["a", "c"]


def test_select_nodes_empty_selectors_returns_all():
    nodes = [_node(node_id="a"), _node(node_id="b")]
    assert select_nodes(nodes, []) == nodes
