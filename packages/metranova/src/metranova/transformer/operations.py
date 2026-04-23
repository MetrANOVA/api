from typing import Literal


class OperationConfigField:
    name: str
    type: type
    required: bool

    def __init__(self, name: str, type: type, required: bool):
        self.name = name
        self.type = type
        self.required = required


class Operation:
    name: str
    category: Literal["Field", "String", "Math", "Cache"]
    config: list[OperationConfigField]

    def __init__(
        self,
        name: str,
        category: Literal["Field", "String", "Math", "Cache"],
        config: list[OperationConfigField],
    ):
        self.name = name
        self.category = category
        self.config = config


operations = {
    "field": Operation(
        name="field",
        category="Field",
        config=[
            OperationConfigField(name="source", type=str, required=True),
            OperationConfigField(name="cast", type=str, required=False),
        ],
    ),
    "static": Operation(
        name="static",
        category="Field",
        config=[OperationConfigField(name="value", required=True, type=str)],
    ),
    "concat": Operation(
        name="concat",
        category="String",
        config=[
            OperationConfigField(name="fields", type=list[str], required=True),
            OperationConfigField(name="delimiter", type=str, required=True),
        ],
    ),
    "regex_extract": Operation(
        name="regex_extract",
        category="String",
        config=[
            OperationConfigField(name="source", type=str, required=True),
            OperationConfigField(name="regex", type=str, required=True),
            OperationConfigField(name="greoup", type=int, required=True),
            OperationConfigField(name="cast", type=str, required=False),
        ],
    ),
    "regex_replace": Operation(
        name="regex_replace",
        category="String",
        config=[
            OperationConfigField(name="source", type=str, required=True),
            OperationConfigField(name="regex", type=str, required=True),
            OperationConfigField(name="replacement", type=str, required=True),
        ],
    ),
    "translate": Operation(
        name="translate",
        category="String",
        config=[
            OperationConfigField(name="source", type=str, required=True),
            OperationConfigField(name="map", type=dict[str, str], required=True),
            OperationConfigField(name="default", type=str, required=True),
        ],
    ),
    "postfix": Operation(
        name="postfix",
        category="Math",
        config=[
            OperationConfigField(name="field", type=str, required=True),
            OperationConfigField(name="expression", type=str, required=True),
            OperationConfigField(name="cast", type=str, required=False),
        ],
    ),
    "cache_lookup": Operation(
        name="cache_lookup",
        category="Cache",
        config=[
            OperationConfigField(name="metadata_type", type=str, required=True),
            OperationConfigField(name="key", type=str, required=True),
            OperationConfigField(name="location", type=str, required=True),
            OperationConfigField(
                name="on_miss", type=Literal["null", "skip", "fail"], required=True
            ),
            OperationConfigField(name="cast", type=str, required=False),
        ],
    ),
}
