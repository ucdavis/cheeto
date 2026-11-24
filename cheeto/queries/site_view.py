"""Site-wide user and group views for the REST API.

Each view reads one collection per query and joins in Python, so the query
count is fixed: it does not grow with the number of users or groups at the
site. `queries/puppet_legacy.py::site_to_puppet_legacy` is the model for the
gather; the differences are deliberate:

- Statuses, user types and the access list are carried as data (the legacy
  export folds them into `tag` or drops them).
- Group roles are kept per group, all four kinds.
- Slurm accounts follow the SYNC definition (`build_desired_slurm_state`):
  a `member` or `slurmer` role on a group that owns a SlurmAccount, a sticky
  group that owns one, or one of the site's sticky accounts. The legacy
  export does not count plain `member` edges.
- Storage is every Storage record (automount or static mount), not only the
  automount rows.
- No filter on links goes through `fetch_links=True`: beanie places the
  `$lookup` stages before the `$match`, so such a find joins the whole
  collection before it filters.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from beanie import PydanticObjectId
from beanie.operators import In

from ..models.base import link_target_id
from ..models.group import SPECIAL_GROUP_CLASSES, Group
from ..models.group_membership import GroupMembership
from ..models.site import Site
from ..models.slurm import SlurmAccount
from ..models.storage import Storage
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from .access_status import access_status_names
from .group import site_present_group_ids
from .slurm import slurm_max_jobs
from .storage import list_site_storages
from .user import effective_access_links, effective_status_link


@dataclass
class SiteUserGroup:
    name: str
    gid: int
    roles: list[str]
    sticky: bool


@dataclass
class SiteUserView:
    user: User
    usi: UserSiteInfo
    status: str | None                 # effective
    access: list[str]                  # effective
    groups: list[SiteUserGroup] = field(default_factory=list)
    slurm_max_jobs: int | None = None  # None: holds no Slurm associations
    slurm_accounts: list[str] = field(default_factory=list)
    home: Storage | None = None


@dataclass
class SiteGroupView:
    group: Group
    sticky: bool
    slurm_account: bool
    members: list[str] = field(default_factory=list)
    sponsors: list[str] = field(default_factory=list)
    sudoers: list[str] = field(default_factory=list)
    slurmers: list[str] = field(default_factory=list)
    storages: list[Storage] = field(default_factory=list)


def _sticky_ids(links) -> set[PydanticObjectId]:
    ids = {link_target_id(link) for link in links}
    ids.discard(None)
    return ids


async def _present_groups(site: Site) -> dict[PydanticObjectId, Group]:
    """Posix groups present at the site (GroupSiteInfo), keyed by id. The
    access/status/type rows are metadata, not groups a user is "in"."""
    ids = await site_present_group_ids(site)
    if not ids:
        return {}
    groups = await Group.find(
        In(Group.id, list(ids)), with_children=True,
    ).to_list()
    return {
        g.id: g for g in groups if not isinstance(g, SPECIAL_GROUP_CLASSES)
    }


async def _site_users(
    site: Site,
) -> tuple[list[UserSiteInfo], dict[PydanticObjectId, User]]:
    usis = await UserSiteInfo.find(UserSiteInfo.site.id == site.id).to_list()
    if not usis:
        return [], {}
    users = await User.find(
        In(User.id, [link_target_id(usi.user) for usi in usis]),
    ).to_list()
    return usis, {u.id: u for u in users}


async def site_users_view(site: Site) -> list[SiteUserView]:
    """Every user with a `UserSiteInfo` at `site`, sorted by name."""
    usis, user_by_id = await _site_users(site)
    if not usis:
        return []

    names = await access_status_names()
    group_by_id = await _present_groups(site)
    sticky_group_ids = _sticky_ids(site.group.sticky) & set(group_by_id)

    edges = await GroupMembership.find(
        GroupMembership.site.id == site.id,
    ).to_list()
    # Unfiltered by presence: the Slurm sync builds an account's roster from
    # the edges alone, so the account list below must do the same.
    roles_by_user: dict[PydanticObjectId, dict[PydanticObjectId, list[str]]] = {}
    for e in edges:
        roles_by_user.setdefault(link_target_id(e.user), {})[
            link_target_id(e.group)
        ] = sorted(e.roles)

    accounts = await SlurmAccount.find(
        SlurmAccount.site.id == site.id,
    ).to_list()
    # An account is named by its owning group. That group normally carries a
    # GroupSiteInfo; resolve the strays so the names are complete.
    account_group_ids = {link_target_id(a.group) for a in accounts}
    account_group_ids.discard(None)
    account_name = {
        gid: group_by_id[gid].name
        for gid in account_group_ids if gid in group_by_id
    }
    missing = account_group_ids - set(account_name)
    if missing:
        extra = await Group.find(
            In(Group.id, list(missing)), with_children=True,
        ).to_list()
        account_name.update({g.id: g.name for g in extra})
    sticky_account_ids = _sticky_ids(site.slurm.sticky)
    sticky_account_names = {
        account_name[gid] for a in accounts
        if a.id in sticky_account_ids
        and (gid := link_target_id(a.group)) in account_name
    }
    all_sticky_group_ids = _sticky_ids(site.group.sticky)

    home_by_owner: dict[PydanticObjectId, Storage] = {}
    for s in await list_site_storages(site, 'home'):
        if s.owner is not None and s.volume is not None:
            home_by_owner.setdefault(s.owner.id, s)

    views: list[SiteUserView] = []
    for usi in usis:
        user = user_by_id.get(link_target_id(usi.user))
        if user is None:
            continue  # dangling UserSiteInfo
        status = names.status_name(effective_status_link(user, usi))
        access = names.access_names(effective_access_links(user, usi))

        roles = roles_by_user.get(user.id, {})
        group_ids = (set(roles) & set(group_by_id)) | sticky_group_ids
        groups = sorted(
            (
                SiteUserGroup(
                    name=group_by_id[gid].name,
                    gid=group_by_id[gid].gid,
                    roles=roles.get(gid, []),
                    sticky=gid in sticky_group_ids,
                )
                for gid in group_ids
            ),
            key=lambda g: g.name,
        )
        slurm_group_ids = {
            gid for gid in account_name
            if gid in all_sticky_group_ids
            or {'member', 'slurmer'} & set(roles.get(gid, ()))
        }
        views.append(SiteUserView(
            user=user, usi=usi, status=status, access=access, groups=groups,
            slurm_max_jobs=slurm_max_jobs(access, status),
            slurm_accounts=sorted(
                {account_name[gid] for gid in slurm_group_ids}
                | sticky_account_names
            ),
            home=home_by_owner.get(user.id),
        ))
    return sorted(views, key=lambda v: v.user.name)


async def site_groups_view(
    site: Site, *, personal: bool = False,
) -> list[SiteGroupView]:
    """Every posix group present at `site` with its roster and storages,
    sorted by name. Personal groups (`type == 'user'`, one per user) are
    left out unless `personal` is set. `members` of a sticky group is the
    whole site population, as in `group_members_at_site`."""
    group_by_id = await _present_groups(site)
    if not personal:
        group_by_id = {
            gid: g for gid, g in group_by_id.items() if g.type != 'user'
        }
    if not group_by_id:
        return []
    sticky_group_ids = _sticky_ids(site.group.sticky)

    edges = await GroupMembership.find(
        GroupMembership.site.id == site.id,
    ).to_list()
    ids_by_role: dict[PydanticObjectId, dict[str, set]] = {}
    for e in edges:
        gid = link_target_id(e.group)
        if gid not in group_by_id:
            continue
        buckets = ids_by_role.setdefault(gid, {})
        for role in e.roles:
            buckets.setdefault(role, set()).add(link_target_id(e.user))

    usis, user_by_id = await _site_users(site)
    site_user_ids = set(user_by_id)
    # Sponsors (and any stray edge) can point at users without a
    # UserSiteInfo here.
    referenced = {
        uid for buckets in ids_by_role.values()
        for ids in buckets.values() for uid in ids
    }
    missing = referenced - site_user_ids
    missing.discard(None)
    name_by_id = {uid: u.name for uid, u in user_by_id.items()}
    if missing:
        extra = await User.find(In(User.id, list(missing))).to_list()
        name_by_id.update({u.id: u.name for u in extra})

    accounts = await SlurmAccount.find(
        SlurmAccount.site.id == site.id,
    ).to_list()
    account_group_ids = {link_target_id(a.group) for a in accounts}

    storages_by_group: dict[PydanticObjectId, list[Storage]] = {}
    for s in await list_site_storages(site, 'group'):
        if s.group is not None and s.volume is not None and s.owner is not None:
            storages_by_group.setdefault(s.group.id, []).append(s)

    def _names(ids) -> list[str]:
        return sorted(
            n for uid in ids if (n := name_by_id.get(uid)) is not None
        )

    views: list[SiteGroupView] = []
    for gid, group in group_by_id.items():
        buckets = ids_by_role.get(gid, {})
        sticky = gid in sticky_group_ids
        member_ids = set(buckets.get('member', ()))
        if sticky:
            member_ids |= site_user_ids
        views.append(SiteGroupView(
            group=group,
            sticky=sticky,
            slurm_account=gid in account_group_ids,
            members=_names(member_ids),
            sponsors=_names(buckets.get('sponsor', ())),
            sudoers=_names(buckets.get('sudoer', ())),
            slurmers=_names(buckets.get('slurmer', ())),
            storages=storages_by_group.get(gid, []),
        ))
    return sorted(views, key=lambda v: v.group.name)
