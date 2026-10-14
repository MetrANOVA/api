"""Paging, search and sort parameters shared by the list endpoints.

List endpoints keep returning a plain JSON list. The number of rows matching
the request (before limit/offset) is reported in the X-Total-Count header.
"""

from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Query

MAX_LIMIT = 500
TOTAL_COUNT_HEADER = "X-Total-Count"


@dataclass
class ListParams:
    limit: int | None = None
    offset: int = 0
    q: str | None = None
    sort: str | None = None
    order: str = "asc"

    @property
    def paged(self) -> bool:
        return self.limit is not None or self.offset > 0

    @property
    def direction(self) -> str:
        return "DESC" if self.order == "desc" else "ASC"


def list_params(
    limit: Annotated[
        int | None,
        Query(ge=1, le=MAX_LIMIT, description="Maximum number of rows to return."),
    ] = None,
    offset: Annotated[
        int, Query(ge=0, description="Number of matching rows to skip.")
    ] = 0,
    q: Annotated[str | None, Query(description="Case-insensitive text search.")] = None,
    sort: Annotated[str | None, Query(description="Column to sort by.")] = None,
    order: Annotated[Literal["asc", "desc"], Query()] = "asc",
) -> ListParams:
    return ListParams(
        limit=limit, offset=offset, q=(q or "").strip() or None, sort=sort, order=order
    )


def limit_clause(params: ListParams, parameters: dict) -> str:
    """Return the LIMIT/OFFSET clause for params and add its values to parameters."""
    clause = ""
    if params.limit is not None:
        clause += " LIMIT {limit:UInt32}"
        parameters["limit"] = params.limit
    if params.offset:
        clause += " OFFSET {offset:UInt32}"
        parameters["offset"] = params.offset
    return clause


def search_clause(columns: list[str], parameters: dict, q: str) -> str:
    """Return a condition matching q in any of the (already quoted) columns."""
    if not columns:
        return "0"
    parameters["q"] = q
    return (
        "("
        + " OR ".join(
            f"positionCaseInsensitiveUTF8({column}, {{q:String}}) > 0"
            for column in columns
        )
        + ")"
    )
