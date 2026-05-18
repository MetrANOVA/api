import dataclasses
import logging
import re

logger = logging.getLogger(__name__)


def _get_field(message: dict, name: str):
    return message.get("fields", {}).get(name) or message.get("tags", {}).get(name)


def _cast(value, cast_type: str | None):
    if cast_type is None or value is None:
        return value
    return {"str": str, "int": int, "float": float}.get(cast_type, lambda v: v)(value)


def _eval_postfix(tokens: list[str], field_name: str, field_value) -> float:
    ops = {
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a / b,
    }
    stack = []
    for token in tokens:
        if token in ops:
            b, a = stack.pop(), stack.pop()
            stack.append(ops[token](a, b))
        elif token == field_name:
            stack.append(float(field_value))
        else:
            stack.append(float(token))
    return stack[0]


@dataclasses.dataclass
class TransformerColumn:
    """A single column transformation rule within a Transformer.

    Describes how to compute the value of `target_column` for a pipeline message.
    `operation` selects the computation (e.g. "field", "static", "concat") and
    `config` holds the operation-specific parameters. `match_value` is compared
    against the parent Transformer's `match_field` in the message metadata to
    decide whether this column applies. Columns within a Transformer are applied
    in ascending `order`.
    """

    id: str
    match_value: str
    target_column: str
    operation: str
    config: dict
    default_value: str = ""
    order: int = 0
    vendor_match_field: str | None = None
    vendor_match_value: str | None = None

    def apply(self, message: dict) -> None:
        """Apply this column's operation to `message`, mutating it in-place.

        Reads source values from `message["fields"]` and `message["tags"]`,
        computes the result according to `self.operation` and `self.config`,
        and writes it to `message["fields"][self.target_column]`. Falls back
        to `self.default_value` when the operation produces `None`.
        """
        c = self.config
        result = None
        try:
            match self.operation:
                case "field":
                    result = _cast(_get_field(message, c["source"]), c.get("cast"))
                case "static":
                    result = c["value"]
                case "concat":
                    parts = [str(_get_field(message, f) or "") for f in c["fields"]]
                    result = c["delimiter"].join(parts)
                case "regex_extract":
                    src = _get_field(message, c["source"])
                    if src is not None:
                        m = re.search(c["regex"], str(src))
                        if m is not None:
                            result = _cast(m.group(c["group"]), c.get("cast"))
                case "regex_replace":
                    src = _get_field(message, c["source"])
                    if src is not None:
                        result = re.sub(c["regex"], c["replacement"], str(src))
                case "translate":
                    src = _get_field(message, c["source"])
                    result = c["map"].get(
                        str(src) if src is not None else "", c["default"]
                    )
                case "postfix":
                    src = _get_field(message, c["field"])
                    if src is not None:
                        tokens = c["expression"].split()
                        result = _cast(
                            _eval_postfix(tokens, c["field"], src), c.get("cast")
                        )
                case "cache_lookup":
                    raise NotImplementedError(
                        "cache_lookup requires external cache state"
                    )
                case _:
                    logger.warning(f"Unknown transformer operation: '{self.operation}'")
        except Exception as e:
            logger.warning(
                f"Transformer operation '{self.operation}' failed for column "
                f"'{self.target_column}': {e}"
            )
            return None

        if result is None and self.default_value:
            result = self.default_value

        message["fields"][self.target_column] = result


@dataclasses.dataclass
class Transformer:
    """A named transformer that applies a set of column rules to pipeline messages.

    A Transformer is associated with a resource definition (`definition_ref`) and
    activated when `match_field` in the message metadata matches the `match_value`
    of one of its `columns`. Columns are ordered and applied in sequence via
    `TransformerColumn.apply`.
    """

    id: str
    name: str
    match_field: str
    definition_ref: str
    columns: list[TransformerColumn]
