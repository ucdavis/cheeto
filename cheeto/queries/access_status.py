"""Resolve and look up `AccessGroup` / `StatusGroup` / `TypeGroup` records.

Resolvers accept a `Link` or an already-fetched document. Lookups take a
shorthand (`access_name` / `status_name` / `user_type`) and return the record
or None. `User.type` is a plain string, not a Link, so the type resolver
takes the string directly.
"""

from __future__ import annotations

from typing import Iterable

from beanie import Link

from ..models.group import AccessGroup, StatusGroup, TypeGroup


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
