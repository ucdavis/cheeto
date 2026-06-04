"""Export a site's users + groups as a v1-compatible PuppetAccountMap.

This is the v2 equivalent of `cheeto.database.crud.site_to_puppet`. It
reads from the beanie collections and produces a
`cheeto.puppet.PuppetAccountMap` that can be serialized via the existing
marshmallow schema (`PuppetAccountMap.Schema().dumps(...)`).

Storage and shares are intentionally out of scope for this pass — v2
has the `Storage` model but no query helpers yet. The output omits
`storage` on user/group records and emits an empty `share: {}` block.

Shell handling matches v1 verbatim: the raw `User.shell` is emitted,
even when the user is inactive/disabled. v1's `user_to_puppet` computes
a translated shell into a local variable that's never read
(`crud.py:1042-1046`); we don't port that translation.
"""

from __future__ import annotations

from beanie.operators import In

from ..models.base import link_target_id
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.group_membership import GroupMembership
from ..models.site import Site
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from ..puppet import (
    PuppetAccountMap,
    PuppetGroupRecord,
    PuppetUserRecord,
)
from ..utils import removed_nones
from .access_status import resolve_access_names
from .slurm import user_slurm_at_site
from .user import effective_access_links


_ACCESS_TO_TAG = {
    'compute-ssh': 'ssh-tag',
    'root-ssh': 'root-ssh-tag',
    'sudo': 'sudo-tag',
}


def _tags_for(user: User, access_names: list[str]) -> list[str] | None:
    tags = {_ACCESS_TO_TAG[name] for name in access_names if name in _ACCESS_TO_TAG}
    if user.type == 'system':
        tags.add('system-tag')
    return sorted(tags) if tags else None


def _expiry_for(user: User, usi: UserSiteInfo) -> str | None:
    when = usi.expires_at or user.expires_at
    if when is None:
        return None
    return when.date().isoformat()


async def site_to_puppet_legacy(site: Site) -> PuppetAccountMap:
    """Build a v1-compatible PuppetAccountMap from v2 data at `site`."""
    usis = await UserSiteInfo.find(UserSiteInfo.site.id == site.id).to_list()
    if not usis:
        return PuppetAccountMap(user={}, group={}, share={})

    user_ids = [usi.user.ref.id for usi in usis]
    usi_by_user_id = {usi.user.ref.id: usi for usi in usis}
    users = await User.find(In(User.id, user_ids)).to_list()
    user_id_set = set(user_ids)

    sticky_group_ids = {
        link_target_id(link) for link in site.group.sticky
    }
    sticky_group_ids.discard(None)

    # Every membership edge at this site, in one query. Roles drive the
    # member/sudoer/sponsor maps below.
    edges = await GroupMembership.find(
        GroupMembership.site.id == site.id,
    ).to_list()

    # Sponsor edges may reference users who aren't present at the site
    # (no UserSiteInfo); v1 emitted sponsor usernames regardless. So we
    # collect every group id referenced by any edge, plus sticky groups.
    referenced_group_ids = {link_target_id(e.group) for e in edges}
    referenced_group_ids |= sticky_group_ids
    referenced_group_ids.discard(None)

    raw_groups = (
        await Group.find(
            In(Group.id, list(referenced_group_ids)), with_children=True,
        ).to_list()
        if referenced_group_ids else []
    )
    # AccessGroup/StatusGroup are v2 metadata, not exportable POSIX groups.
    candidate_groups = [
        g for g in raw_groups
        if not isinstance(g, (AccessGroup, StatusGroup))
    ]
    group_by_id = {g.id: g for g in candidate_groups}

    member_groups_by_user: dict = {uid: set() for uid in user_ids}
    sudoer_groups_by_user: dict = {uid: set() for uid in user_ids}
    sponsor_ids_by_group: dict = {}
    for e in edges:
        gid = link_target_id(e.group)
        if gid not in group_by_id:
            continue  # access/status group or otherwise not exportable
        uid = link_target_id(e.user)
        if 'sponsor' in e.roles:
            sponsor_ids_by_group.setdefault(gid, set()).add(uid)
        if uid not in user_id_set:
            continue  # member/sudoer rows only matter for at-site users
        if 'member' in e.roles:
            member_groups_by_user[uid].add(gid)
        if 'sudoer' in e.roles:
            sudoer_groups_by_user[uid].add(gid)
    # Sticky groups: every at-site user is implicitly a member.
    for gid in sticky_group_ids:
        if gid in group_by_id:
            for uid in user_ids:
                member_groups_by_user[uid].add(gid)

    # Identify primary groups for at-site users — excluded from both
    # the user record's `groups` and the top-level `group:` map (matches
    # v1's skip at crud.py:1140-1144).
    primary_group_ids: set = set()
    for u in users:
        for gid in member_groups_by_user[u.id]:
            g = group_by_id[gid]
            if g.name == u.name and g.gid == u.uid:
                primary_group_ids.add(gid)

    # Batch sponsor name resolution across every sponsor edge.
    sponsor_ids = {
        sid for ids in sponsor_ids_by_group.values()
        for sid in ids if sid is not None
    }
    sponsor_name_by_id = {u.id: u.name for u in users if u.id in sponsor_ids}
    missing_sponsor_ids = sponsor_ids - set(sponsor_name_by_id.keys())
    if missing_sponsor_ids:
        extras = await User.find(
            In(User.id, list(missing_sponsor_ids)),
        ).to_list()
        sponsor_name_by_id.update({u.id: u.name for u in extras})

    user_records: dict[str, PuppetUserRecord] = {}
    for u in users:
        usi = usi_by_user_id[u.id]

        eff_access_links = effective_access_links(u, usi)
        access_names = await resolve_access_names(eff_access_links)

        member_names = sorted({
            group_by_id[gid].name
            for gid in member_groups_by_user[u.id]
            if group_by_id[gid].name != u.name
        })
        sudoer_names = sorted({
            group_by_id[gid].name
            for gid in sudoer_groups_by_user[u.id]
        })

        slurm_rows = await user_slurm_at_site(u, site)
        account_names = sorted({row.group.name for row in slurm_rows})

        record_data = removed_nones({
            'fullname': u.fullname,
            'email': u.email,
            'uid': u.uid,
            'gid': u.gid,
            'groups': member_names or None,
            'group_sudo': sudoer_names or None,
            'password': u.password,
            'shell': u.shell,
            'tag': _tags_for(u, access_names),
            'home': u.home_directory,
            'expiry': _expiry_for(u, usi),
            'slurm': {'account': account_names} if account_names else None,
        })
        user_records[u.name] = PuppetUserRecord.load(record_data)

    group_records: dict[str, PuppetGroupRecord] = {}
    for g in candidate_groups:
        if g.id in primary_group_ids:
            continue
        sponsor_names = sorted({
            n for sid in sponsor_ids_by_group.get(g.id, set())
            if (n := sponsor_name_by_id.get(sid)) is not None
        })
        record_data = removed_nones({
            'gid': g.gid,
            'sponsors': sponsor_names or None,
        })
        group_records[g.name] = PuppetGroupRecord.load(record_data)

    return PuppetAccountMap(
        user=user_records,
        group=group_records,
        share={},
    )
