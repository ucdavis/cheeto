"""Export a site's users + groups as a v1-compatible PuppetAccountMap.

This is the v2 equivalent of `cheeto.database.crud.site_to_puppet`. It
reads from the beanie collections and produces a
`cheeto.puppet.PuppetAccountMap` that can be serialized via the existing
marshmallow schema (`PuppetAccountMap.Schema().dumps(...)`).

Storage follows v1's automount-table projection (`crud.py`
user_to_puppet/group_to_puppet/share_to_puppet): the user record carries
its home storage (`autofs` nas + parent path, `zfs` quota or false),
group records carry a `storage:` list of their group-category automount
rows, and share-category rows become the `share:` map. Storages without
an automount map (static-mount sites) are not projected, matching v1.

Shell handling matches v1 verbatim: the raw `User.shell` is emitted,
even when the user is inactive/disabled. v1's `user_to_puppet` computes
a translated shell into a local variable that's never read
(`crud.py:1042-1046`); we don't port that translation.
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath

from beanie.operators import In

from ..models.base import link_target_id
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.group_membership import GroupMembership
from ..models.site import Site
from ..models.slurm import SlurmAccount
from ..models.storage import Storage
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from ..puppet import (
    PuppetAccountMap,
    PuppetGroupRecord,
    PuppetShareRecord,
    PuppetUserRecord,
)
from ..utils import removed_nones
from .access_status import resolve_access_names
from .slurm import user_slurm_at_site
from .storage import list_automap_storages
from .user import effective_access_links

logger = logging.getLogger(__name__)


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


def _puppet_zfs(storage: Storage) -> dict | bool:
    """v1 `get_puppet_zfs` parity: a quota'd managed dataset renders as
    `{quota: ...}`, anything else (quobyte, bare/unmanaged volume, subpath
    export — whose `quota` property is already None) as `false`."""
    if storage.volume.zfs is not None and storage.quota:
        return {'quota': storage.quota}
    return False


def _autofs_options(storage: Storage) -> str:
    # v1 strips the fstype token and joins reverse-sorted (crud.py
    # group_to_puppet).
    return ','.join(
        sorted(set(storage.mount_options) - {'fstype=nfs'}, reverse=True)
    )


async def site_to_puppet_legacy(site: Site) -> PuppetAccountMap:
    """Build a v1-compatible PuppetAccountMap from v2 data at `site`."""
    usis = await UserSiteInfo.find(UserSiteInfo.site.id == site.id).to_list()

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

    # Automount-table storage projections (v1 user_to_puppet /
    # group_to_puppet / share_to_puppet). Depth-1 link fetch resolves
    # volume/automount_map/owner/group, which is all the blocks need.
    home_by_owner: dict = {}
    for s in await list_automap_storages(site, 'home'):
        home_by_owner.setdefault(link_target_id(s.owner), s)

    storages_by_group_id: dict = {}
    for s in await list_automap_storages(site, 'group'):
        gid = link_target_id(s.group)
        storages_by_group_id.setdefault(gid, []).append(s)
        # v1 exported every SiteGroup; a group whose only site presence is
        # its storage must still appear in the group map.
        if gid not in group_by_id and not isinstance(
            s.group, (AccessGroup, StatusGroup),
        ):
            group_by_id[gid] = s.group
            candidate_groups.append(s.group)

    share_storages = sorted(
        await list_automap_storages(site, 'share'), key=lambda s: s.name,
    )

    # Groups with a slurm account at this site are site-present even with
    # no member edges (v1 had a SiteGroup for every sponsor group with an
    # account); they must appear in the group map.
    slurm_accounts = await SlurmAccount.find(
        SlurmAccount.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    ).to_list()
    for account in slurm_accounts:
        g = account.group
        if g.id not in group_by_id and not isinstance(
            g, (AccessGroup, StatusGroup),
        ):
            group_by_id[g.id] = g
            candidate_groups.append(g)

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
    # v1's skip at crud.py:1140-1144). Checked over every candidate group
    # (not just member edges) so a storage-owning primary group is still
    # excluded.
    user_name_uid = {(u.name, u.uid) for u in users}
    primary_group_ids: set = {
        g.id for g in candidate_groups
        if (g.name, g.gid) in user_name_uid
    }

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

        home = home_by_owner.get(u.id)
        if home is not None:
            storage_data = {
                'autofs': {
                    'nas': home.host,
                    'path': str(PurePosixPath(home.host_path).parent),
                },
                'zfs': _puppet_zfs(home),
            }
        else:
            storage_data = None
            logger.warning(
                'No home storage found for %s at %s', u.name, site.name,
            )

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
            'storage': storage_data,
        })
        user_records[u.name] = PuppetUserRecord.load(record_data)

    # v1 exported the special access/status groups (every site carried
    # their SiteGroups); v2 keeps them as global AccessGroup/StatusGroup
    # records. Added after the edge-attribution maps so they never count
    # toward user posix memberships — they render as plain gid records.
    for g in (
        await AccessGroup.find_all().to_list()
        + await StatusGroup.find_all().to_list()
    ):
        if g.id not in group_by_id:
            group_by_id[g.id] = g
            candidate_groups.append(g)

    group_records: dict[str, PuppetGroupRecord] = {}
    for g in candidate_groups:
        if g.id in primary_group_ids:
            continue
        sponsor_names = sorted({
            n for sid in sponsor_ids_by_group.get(g.id, set())
            if (n := sponsor_name_by_id.get(sid)) is not None
        })
        # v1 emits the storage list unconditionally (empty when the group
        # owns no automounted storage at the site).
        storage_rows = [
            {
                'name': s.name,
                'owner': s.owner.name,
                'group': s.group.name,
                'autofs': {
                    'nas': s.host,
                    'path': s.host_path,
                    'options': _autofs_options(s),
                },
                'zfs': _puppet_zfs(s),
                'globus': s.globus,
            }
            for s in sorted(
                storages_by_group_id.get(g.id, []), key=lambda s: s.name,
            )
        ]
        record_data = removed_nones({
            'gid': g.gid,
            'sponsors': sponsor_names or None,
            'storage': storage_rows,
        })
        group_records[g.name] = PuppetGroupRecord.load(record_data)

    share_records = {
        s.name: PuppetShareRecord.load({'storage': {
            'owner': s.owner.name,
            'group': s.group.name,
            'autofs': {'nas': s.host, 'path': s.host_path},
            'zfs': _puppet_zfs(s),
        }})
        for s in share_storages
    }

    return PuppetAccountMap(
        user=user_records,
        group=group_records,
        share=share_records,
    )
