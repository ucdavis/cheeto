"""History (audit log) query helpers."""

from __future__ import annotations

from datetime import datetime

from beanie import PydanticObjectId

from ..models.history import History


async def find_history(
    *,
    op: str | None = None,
    author_id: PydanticObjectId | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 50,
) -> list[History]:
    """History entries matching the given filters, newest first.

    `since`/`until` are inclusive bounds on `timestamp`. `fetch_links`
    resolves each entry's author in the initial aggregation (avoids an extra
    round-trip per row in the caller's render loop).
    """
    filters = []
    if op is not None:
        filters.append(History.op == op)
    if author_id is not None:
        filters.append(History.author.id == author_id)
    if since is not None:
        filters.append(History.timestamp >= since)
    if until is not None:
        filters.append(History.timestamp <= until)
    return await (
        History.find(*filters, fetch_links=True, nesting_depth=1)
        .sort('-timestamp')
        .limit(limit)
        .to_list()
    )
