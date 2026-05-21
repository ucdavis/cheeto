"""Group-membership query helpers, including `Site.group.sticky` union logic.

`effective_group_members(group, site)` is the central join: when `group` is
listed in `site.group.sticky`, every user with a `UserSiteInfo` at the site
counts as a member even if they're not in `group.members`. Used by LDAP
projection (`SyncGroupToLDAP._members_at_site`) and the `--site`-scoped
group filter on `find_users`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from beanie import PydanticObjectId
from beanie.operators import In, Or

from ..models.group import Group
from ..models.base import link_target_id
from ..models.site import Site
from ..models.user import User
from ..models.user_site_info import UserSiteInfo


GroupRole = Literal['member', 'slurmer', 'sticky-member']


@dataclass
class UserGroupRoles:
    """A group the user is in at a site, with the role(s) by which they're in it."""
    group: Group
    roles: list[GroupRole] = field(default_factory=list)


def _sticky_group_ids(site: Site) -> set[PydanticObjectId]:
    return {link_target_id(link) for link in site.group.sticky}


def is_sticky_group(group: Group, site: Site) -> bool:
    """True when `group` appears in `site.group.sticky`."""
    return group.id in _sticky_group_ids(site)


async def find_group_by_name(
    name: str, *, with_children: bool = True,
    fetch_links: bool = False,
) -> Group | None:
    """Look up a group by name. `with_children=True` (default) makes the
    polymorphic find see `AccessGroup` / `StatusGroup` rows too."""
    return await Group.find_one(
        Group.name == name,
        with_children=with_children,
        fetch_links=fetch_links,
    )


async def resolve_group_names(links) -> list[str]:
    """Resolve a list of `Link[Group]` (fetched or unfetched) to a sorted
    list of group names. Batches all unresolved ids into a single query."""
    ids: list[PydanticObjectId] = []
    names: list[str] = []
    for link in links:
        if isinstance(link, Group):
            names.append(link.name)
            continue
        target_id = link_target_id(link)
        if target_id is not None:
            ids.append(target_id)
    if ids:
        groups = await Group.find(
            In(Group.id, ids), with_children=True,
        ).to_list()
        names.extend(g.name for g in groups)
    return sorted(names)


async def _users_at_site_ids(site: Site) -> set[PydanticObjectId]:
    usis = await UserSiteInfo.find(UserSiteInfo.site.id == site.id).to_list()
    return {usi.user.ref.id for usi in usis}


async def effective_group_members(
    group: Group, site: Site,
) -> set[PydanticObjectId]:
    """User ids effectively members of `group` at `site`.

    Unions `group.members` with every user that has a `UserSiteInfo` at
    `site` when `group` is listed in `site.group.sticky`. Returns just
    `group.members` otherwise.
    """
    direct = {
        m.id if isinstance(m, User) else m.ref.id for m in group.members
    }
    if not is_sticky_group(group, site):
        return direct
    return direct | await _users_at_site_ids(site)


async def _user_has_usi(user: User, site: Site) -> bool:
    usi = await UserSiteInfo.find_one(
        UserSiteInfo.user.id == user.id,
        UserSiteInfo.site.id == site.id,
    )
    return usi is not None


async def effective_user_groups(
    user: User, site: Site,
) -> tuple[list[Group], set[PydanticObjectId]]:
    """All groups the user is effectively a member of at `site`.

    Returns `(groups, sticky_ids)` where `groups` is the union of the
    user's direct memberships and every group in `site.group.sticky`
    (gated on the user having a `UserSiteInfo` at the site — sticky
    membership only applies to users actually present at the site), and
    `sticky_ids` is the set of group ids that came from the sticky list
    (so callers can tell sticky from direct).
    """
    sticky_ids = _sticky_group_ids(site)
    if sticky_ids and await _user_has_usi(user, site):
        groups = await Group.find(
            Or(Group.members.id == user.id, In(Group.id, list(sticky_ids))),
        ).to_list()
    else:
        sticky_ids = set()
        groups = await Group.find(Group.members.id == user.id).to_list()
    return groups, sticky_ids


async def user_groups_at_site(
    user: User, site: Site,
) -> list[UserGroupRoles]:
    """All groups the user has any membership in at `site`, with roles.

    Each `UserGroupRoles` row has one or more roles from:
      - `'member'`     — user in `group.members`
      - `'slurmer'`    — user in `group.slurmers`
      - `'sticky-member'` — `group` is in `site.group.sticky` and the user
                            has a `UserSiteInfo` at the site

    A group can carry multiple roles (e.g. both `member` and `slurmer`).
    """
    member_groups, sticky_group_ids = await effective_user_groups(user, site)
    slurmer_groups = await Group.find(
        Group.slurmers.id == user.id,
    ).to_list()

    by_id: dict[PydanticObjectId, UserGroupRoles] = {}
    for g in member_groups:
        entry = by_id.setdefault(g.id, UserGroupRoles(group=g))
        direct_member_ids = {link_target_id(m) for m in g.members}
        if user.id in direct_member_ids:
            entry.roles.append('member')
        if g.id in sticky_group_ids:
            entry.roles.append('sticky-member')
    for g in slurmer_groups:
        entry = by_id.setdefault(g.id, UserGroupRoles(group=g))
        entry.roles.append('slurmer')
    return list(by_id.values())
