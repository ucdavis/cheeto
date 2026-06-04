"""User lookup helpers — single-user finders and multi-filter listing.

`find_users` walks each filter into a set of user IDs and combines them
with AND/OR. Cross-collection filters (site, group) join through
UserSiteInfo / Group rather than via a single aggregation pipeline.
"""

from __future__ import annotations

from typing import Literal

from beanie import PydanticObjectId
from beanie.operators import In

from ..models.base import link_target_id
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.group_membership import GroupMembership
from ..models.site import Site
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo


Operator = Literal['AND', 'OR']


def effective_access_links(
    user: User, usi: UserSiteInfo | None,
) -> list:
    """Pick the access list that applies for `user` at the site of `usi`.
    Non-empty `usi.access` overrides; empty falls back to `user.access`."""
    if usi is not None and usi.access:
        return list(usi.access)
    return list(user.access)


async def list_user_ssh_keys(user: User) -> list[SshKey]:
    """Every SshKey belonging to `user`."""
    return await SshKey.find(SshKey.user.id == user.id).to_list()


# ---------------------------------------------------------------------------
# Single-user finders
# ---------------------------------------------------------------------------


async def find_user_by_name(name: str) -> User | None:
    return await User.find_one(User.name == name)


async def find_user_by_uid(uid: int) -> User | None:
    return await User.find_one(User.uid == uid)


async def find_user_by_email(email: str) -> User | None:
    return await User.find_one(User.email == email)


async def find_user(
    *,
    name: str | None = None,
    uid: int | None = None,
    email: str | None = None,
) -> User | None:
    """Look up a user by name, uid, or email. Exactly one identifier
    must be non-None; otherwise raises `ValueError`."""
    provided = [
        k for k, v in (('name', name), ('uid', uid), ('email', email))
        if v is not None
    ]
    if len(provided) != 1:
        raise ValueError(
            f'find_user requires exactly one of name/uid/email; got '
            f'{provided!r}'
        )
    if name is not None:
        return await find_user_by_name(name)
    if uid is not None:
        return await find_user_by_uid(uid)
    return await find_user_by_email(email)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Multi-filter listing
# ---------------------------------------------------------------------------


async def _ids_with_status(
    status_name: str, sitename: str | None = None,
) -> set[PydanticObjectId]:
    """Users whose global `User.status` resolves to `status_name`. When
    `sitename` is supplied, additionally union in users whose
    `UserSiteInfo.status` for that site matches (the per-site override
    fires regardless of what the global status is)."""
    sg = await StatusGroup.find_one(StatusGroup.status_name == status_name)
    if sg is None:
        return set()
    users = await User.find(User.status.id == sg.id).to_list()
    ids = {u.id for u in users}
    if sitename is not None:
        site = await Site.find_one(Site.name == sitename)
        if site is not None:
            usis = await UserSiteInfo.find(
                UserSiteInfo.site.id == site.id,
                UserSiteInfo.status.id == sg.id,
            ).to_list()
            ids |= {usi.user.ref.id for usi in usis}
    return ids


async def _ids_with_access(
    access_name: str, sitename: str | None = None,
) -> set[PydanticObjectId]:
    """Users whose effective access at the scope includes `access_name`.

    Without `sitename`: matches users whose global `User.access` contains
    the access link.

    With `sitename`: applies override semantics. A user matches if either
      (a) they have a `UserSiteInfo` at the site with non-empty
          `access` that contains the access link, OR
      (b) they have a `UserSiteInfo` at the site with empty `access`
          AND their global `User.access` contains the access link.
    Users without a `UserSiteInfo` at the site never match (they have
    no presence there).
    """
    ag = await AccessGroup.find_one(AccessGroup.access_name == access_name)
    if ag is None:
        return set()
    if sitename is None:
        users = await User.find(User.access.id == ag.id).to_list()
        return {u.id for u in users}

    site = await Site.find_one(Site.name == sitename)
    if site is None:
        return set()

    # Phase 1: usi.access overrides and contains the target.
    overriding = await UserSiteInfo.find(
        UserSiteInfo.site.id == site.id,
        UserSiteInfo.access.id == ag.id,
    ).to_list()
    ids = {usi.user.ref.id for usi in overriding}

    # Phase 2: usi.access is empty → fall through to user.access.
    fallback = await UserSiteInfo.find(
        UserSiteInfo.site.id == site.id,
        {'access': {'$size': 0}},
    ).to_list()
    fallback_user_ids = [usi.user.ref.id for usi in fallback]
    if fallback_user_ids:
        users = await User.find(
            In(User.id, fallback_user_ids),
            User.access.id == ag.id,
        ).to_list()
        ids |= {u.id for u in users}

    return ids


async def _ids_with_type(type_str: str) -> set[PydanticObjectId]:
    users = await User.find(User.type == type_str).to_list()
    return {u.id for u in users}


async def _ids_at_site(site_name: str) -> set[PydanticObjectId]:
    site = await Site.find_one(Site.name == site_name)
    if site is None:
        return set()
    usis = await UserSiteInfo.find(UserSiteInfo.site.id == site.id).to_list()
    return {usi.user.ref.id for usi in usis}


async def _ids_in_group(
    group_name: str, sitename: str | None = None,
) -> set[PydanticObjectId]:
    """Users who are `member`s of the named group. Without `sitename`,
    unions `member`-role edges across every site. With `sitename`, scopes
    to that site and unions in the whole-site population if the group is in
    `site.group.sticky` (via `effective_group_members`)."""
    group = await Group.find_one(
        Group.name == group_name, with_children=True,
    )
    if group is None:
        return set()
    if sitename is None:
        edges = await GroupMembership.find(
            GroupMembership.group.id == group.id,
        ).to_list()
        return {
            link_target_id(e.user) for e in edges if 'member' in e.roles
        }
    site = await Site.find_one(Site.name == sitename)
    if site is None:
        return set()
    from .group import effective_group_members
    return await effective_group_members(group, site)


async def find_users(
    *,
    status: str | None = None,
    access: str | None = None,
    type: str | None = None,
    site: str | None = None,
    group: str | None = None,
    operator: Operator = 'AND',
) -> list[User]:
    """Return users matching the given filters combined by `operator`.

    Each filter is optional; when none are given, returns every user
    sorted by name. Filters that resolve to no records produce empty
    ID sets, which under AND wipes the result and under OR leaves the
    remaining filters unaffected.

    When `site` is also supplied, the status and access filters expand
    to include users whose per-site `UserSiteInfo` overrides match, not
    just users whose global `User.status` / `User.access` match.
    """
    if operator not in ('AND', 'OR'):
        raise ValueError(f'operator must be AND or OR; got {operator!r}')

    id_sets: list[set[PydanticObjectId]] = []
    if status is not None:
        id_sets.append(await _ids_with_status(status, sitename=site))
    if access is not None:
        id_sets.append(await _ids_with_access(access, sitename=site))
    if type is not None:
        id_sets.append(await _ids_with_type(type))
    if site is not None:
        id_sets.append(await _ids_at_site(site))
    if group is not None:
        id_sets.append(await _ids_in_group(group, sitename=site))

    if not id_sets:
        return await User.find_all().sort('+name').to_list()

    if operator == 'AND':
        ids = set.intersection(*id_sets)
    else:
        ids = set.union(*id_sets)

    if not ids:
        return []
    return await User.find(
        In(User.id, list(ids)),
    ).sort('+name').to_list()
