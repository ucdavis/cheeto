"""Resolve and look up `AccessGroup` / `StatusGroup` / `TypeGroup` records.

Resolvers accept a `Link` or an already-fetched document. Lookups take a
shorthand (`access_name` / `status_name` / `user_type`) and return the record
or None. `User.type` is a plain string, not a Link, so the type resolver
takes the string directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from beanie import Link, PydanticObjectId
from beanie.operators import In

from ..models.base import link_target_id
from ..models.group import AccessGroup, Group, StatusGroup, TypeGroup


async def find_access_group(access_name: str) -> AccessGroup | None:
    return await AccessGroup.find_one(AccessGroup.access_name == access_name)


async def find_status_group(status_name: str) -> StatusGroup | None:
    return await StatusGroup.find_one(StatusGroup.status_name == status_name)


async def find_type_group(user_type: str) -> TypeGroup | None:
    return await TypeGroup.find_one(TypeGroup.user_type == user_type)


async def resolve_type_ldapname(user_type: str | None) -> str | None:
    if user_type is None:
        return None
    tg = await find_type_group(user_type)
    return tg.name if tg is not None else None


async def resolve_status_name(
    status: Link[StatusGroup] | StatusGroup | None,
) -> str | None:
    if status is None:
        return None
    if isinstance(status, StatusGroup):
        return status.status_name
    sg = await StatusGroup.get(status.ref.id)
    return sg.status_name if sg is not None else None


async def resolve_status_ldapname(
    status: Link[StatusGroup] | StatusGroup | None,
) -> str | None:
    if status is None:
        return None
    if isinstance(status, StatusGroup):
        return status.name
    sg = await StatusGroup.get(status.ref.id)
    return sg.name if sg is not None else None


async def resolve_access_names(
    access_links: Iterable[Link[AccessGroup] | AccessGroup],
) -> list[str]:
    out: list[str] = []
    for link in access_links:
        if isinstance(link, AccessGroup):
            out.append(link.access_name)
            continue
        ag = await AccessGroup.get(link.ref.id)
        if ag is not None:
            out.append(ag.access_name)
    return out


async def resolve_access_ldapnames(
    access_links: Iterable[Link[AccessGroup] | AccessGroup],
) -> list[str]:
    out: list[str] = []
    for link in access_links:
        if isinstance(link, AccessGroup):
            out.append(link.name)
            continue
        ag = await AccessGroup.get(link.ref.id)
        if ag is not None:
            out.append(ag.name)
    return out


@dataclass
class AccessStatusNames:
    """Id -> shorthand maps for every AccessGroup and StatusGroup, for
    readers that resolve many links (the batched form of
    `resolve_access_names` / `resolve_status_name`: no query per link)."""

    access: dict[PydanticObjectId, str] = field(default_factory=dict)
    status: dict[PydanticObjectId, str] = field(default_factory=dict)

    def status_name(self, link) -> str | None:
        if link is None:
            return None
        if isinstance(link, StatusGroup):
            return link.status_name
        return self.status.get(link_target_id(link))

    def access_names(self, links) -> list[str]:
        """Sorted shorthands; a dangling link is skipped."""
        names: list[str] = []
        for link in links:
            if isinstance(link, AccessGroup):
                names.append(link.access_name)
                continue
            name = self.access.get(link_target_id(link))
            if name is not None:
                names.append(name)
        return sorted(names)


async def access_status_names() -> AccessStatusNames:
    """Load the access and status shorthand maps in one query."""
    groups = await Group.find(
        In(Group.type, ['access', 'status']), with_children=True,
    ).to_list()
    names = AccessStatusNames()
    for g in groups:
        if isinstance(g, AccessGroup):
            names.access[g.id] = g.access_name
        elif isinstance(g, StatusGroup):
            names.status[g.id] = g.status_name
    return names
