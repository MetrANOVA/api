from typing import Literal, get_origin, get_args


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
            OperationConfigField(name="group", type=int, required=True),
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


def _check_type(value, expected_type):
    """Check if a value matches the expected type, supporting generic types."""
    # Handle Literal types
    origin = get_origin(expected_type)
    if origin is Literal:
        allowed_values = get_args(expected_type)
        return value in allowed_values

    # Handle list[T]
    if origin is list:
        if not isinstance(value, list):
            return False
        args = get_args(expected_type)
        if args:
            element_type = args[0]
            return all(isinstance(item, element_type) for item in value)
        return True

    # Handle dict[K, V]
    if origin is dict:
        if not isinstance(value, dict):
            return False
        args = get_args(expected_type)
        if args and len(args) >= 2:
            key_type, value_type = args[0], args[1]
            return all(
                isinstance(k, key_type) and isinstance(v, value_type)
                for k, v in value.items()
            )
        return True

    # Handle primitive types
    return isinstance(value, expected_type)


def validate_config(operation_name: str, config: dict) -> tuple[bool, str | None]:
    """
    Validate config against an operation's schema.

    Returns:
        tuple[bool, str | None]: (is_valid, error_message)
    """
    if operation_name not in operations:
        return False, f"Unknown operation: {operation_name}"

    operation = operations[operation_name]
    errors = []

    # Check required fields are present
    for field in operation.config:
        if field.required and field.name not in config:
            errors.append(f"Required field '{field.name}' is missing")

    # Check field types
    for field in operation.config:
        if field.name in config:
            value = config[field.name]
            if not _check_type(value, field.type):
                expected_type_name = str(field.type).replace("typing.", "")
                errors.append(
                    f"Field '{field.name}' has invalid type. Expected {expected_type_name}, got {type(value).__name__}"
                )

    if errors:
        return False, "; ".join(errors)

    return True, None
