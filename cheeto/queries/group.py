"""Group-membership query helpers over per-site `GroupMembership` edges.

Membership is per-site: a `GroupMembership(user, group, site, roles)` edge
records the capacities in which a user participates in a group at a site.
`effective_group_members(group, site)` is the central join — it also unions
in `site.group.sticky` so that when `group` is sticky, every user with a
`UserSiteInfo` at the site counts as a member even without an explicit edge.
Used by LDAP projection (`SyncGroupToLDAP._members_at_site`) and the
`--site`-scoped group filter on `find_users`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from beanie import PydanticObjectId
from beanie.operators import In

from ..models.base import link_target_id
from ..models.group import Group
from ..models.group_membership import GroupMembership
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


async def _user_has_usi(user: User, site: Site) -> bool:
    usi = await UserSiteInfo.find_one(
        UserSiteInfo.user.id == user.id,
        UserSiteInfo.site.id == site.id,
    )
    return usi is not None


async def effective_group_members(
    group: Group, site: Site,
) -> set[PydanticObjectId]:
    """User ids effectively members of `group` at `site`.

    Unions the `member`-role edges for `(group, site)` with every user that
    has a `UserSiteInfo` at `site` when `group` is listed in
    `site.group.sticky`. Returns just the direct members otherwise.
    """
    edges = await GroupMembership.find(
        GroupMembership.group.id == group.id,
        GroupMembership.site.id == site.id,
    ).to_list()
    direct = {
        link_target_id(e.user) for e in edges if 'member' in e.roles
    }
    if not is_sticky_group(group, site):
        return direct
    return direct | await _users_at_site_ids(site)


async def group_members_at_site(
    group: Group, site: Site,
) -> dict[str, list[str]]:
    """The per-site roster of `group`: sorted usernames bucketed by role
    (`members`, `sponsors`, `sudoers`, `slurmers`).

    `members` unions in the whole-site population when `group` is sticky at
    `site` (the same semantics as `effective_group_members`). The other
    buckets reflect explicit edges only.
    """
    edges = await GroupMembership.find(
        GroupMembership.group.id == group.id,
        GroupMembership.site.id == site.id,
    ).to_list()

    ids_by_role: dict[str, set[PydanticObjectId]] = {
        'member': set(), 'sponsor': set(), 'sudoer': set(), 'slurmer': set(),
    }
    for e in edges:
        uid = link_target_id(e.user)
        for role in e.roles:
            ids_by_role[role].add(uid)

    if is_sticky_group(group, site):
        ids_by_role['member'] |= await _users_at_site_ids(site)

    all_ids = set().union(*ids_by_role.values())
    name_by_id: dict[PydanticObjectId, str] = {}
    if all_ids:
        users = await User.find(In(User.id, list(all_ids))).to_list()
        name_by_id = {u.id: u.name for u in users}

    def _names(role: str) -> list[str]:
        return sorted(
            n for uid in ids_by_role[role]
            if (n := name_by_id.get(uid)) is not None
        )

    return {
        'members': _names('member'),
        'sponsors': _names('sponsor'),
        'sudoers': _names('sudoer'),
        'slurmers': _names('slurmer'),
    }


async def effective_user_groups(
    user: User, site: Site,
) -> tuple[list[Group], set[PydanticObjectId]]:
    """All groups the user is effectively a member of at `site`.

    Returns `(groups, sticky_ids)` where `groups` is the union of the user's
    `member`-role edges at the site and every group in `site.group.sticky`
    (sticky gated on the user having a `UserSiteInfo` at the site — sticky
    membership only applies to users actually present there), and
    `sticky_ids` is the set of group ids that came from the sticky list (so
    callers can tell sticky from direct).
    """
    edges = await GroupMembership.find(
        GroupMembership.user.id == user.id,
        GroupMembership.site.id == site.id,
    ).to_list()
    group_ids = {
        link_target_id(e.group) for e in edges if 'member' in e.roles
    }

    sticky_ids = _sticky_group_ids(site)
    if sticky_ids and await _user_has_usi(user, site):
        group_ids |= sticky_ids
    else:
        sticky_ids = set()

    groups = (
        await Group.find(In(Group.id, list(group_ids))).to_list()
        if group_ids else []
    )
    return groups, sticky_ids


async def user_groups_at_site(
    user: User, site: Site,
) -> list[UserGroupRoles]:
    """All groups the user has any membership in at `site`, with roles.

    Each `UserGroupRoles` row has one or more roles from:
      - `'member'`        — a `member`-role edge for `(user, group, site)`
      - `'slurmer'`       — a `slurmer`-role edge for `(user, group, site)`
      - `'sticky-member'` — `group` is in `site.group.sticky` and the user
                            has a `UserSiteInfo` at the site

    Only `member`/`slurmer` are surfaced from edge roles (sponsor/sudoer are
    not group memberships in the posix sense); a group can carry multiple
    roles (e.g. both `member` and `slurmer`).
    """
    edges = await GroupMembership.find(
        GroupMembership.user.id == user.id,
        GroupMembership.site.id == site.id,
    ).to_list()

    roles_by_group: dict[PydanticObjectId, list[GroupRole]] = {}
    for e in edges:
        gid = link_target_id(e.group)
        roles: list[GroupRole] = []
        if 'member' in e.roles:
            roles.append('member')
        if 'slurmer' in e.roles:
            roles.append('slurmer')
        if roles:
            roles_by_group[gid] = roles

    sticky_ids = _sticky_group_ids(site)
    if sticky_ids and await _user_has_usi(user, site):
        for gid in sticky_ids:
            roles_by_group.setdefault(gid, []).append('sticky-member')

    if not roles_by_group:
        return []

    groups = await Group.find(
        In(Group.id, list(roles_by_group.keys())), with_children=True,
    ).to_list()
    return [
        UserGroupRoles(group=g, roles=roles_by_group[g.id])
        for g in groups
    ]
