"""LDAP sync operations using the AsyncLDAPManager.

Six ops mirroring the IAM ops layout in `cheeto/operations/iam.py`:

- `BootstrapLDAPSite` — create the per-site OU tree, automount maps, and
  the LDAP-side entries for every AccessGroup/StatusGroup record in beanie.
- `SyncUserToLDAP` — project one User to LDAP (upsert dn + reconcile
  access/status group memberships).
- `SyncGroupToLDAP` — project one beanie Group to LDAP at one site.
- `SyncSiteAutomounts` — refresh home/group automount entries from beanie
  Storage rows.
- `PruneSiteLDAP` — delete LDAP entries that have no corresponding beanie
  record, with a max_deletions safety cap and dry-run preview.
- `SyncSiteLDAP` — driver that runs the per-record ops + prune.

All ops report tally / outcome dicts via describe() for History tracking.
Transient errors (`LDAPTransientError`) propagate out of inner ops so the
driver can count them and continue.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..ldap_async import (
    AUTO_GROUP,
    AUTO_HOME,
    AsyncLDAPManager,
    LDAPAlreadyExists,
    LDAPGroupRecord,
    LDAPNotFound,
    LDAPPruneAborted,
    LDAPTransientError,
    LDAPUserRecord,
)
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.site import Site
from ..models.storage import Storage
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo
from ..queries.access_status import (
    resolve_access_ldapnames,
    resolve_status_ldapname,
)
from ..queries.user import effective_access_links
from .base import Operation

logger = logging.getLogger(__name__)


_PRUNE_DEFAULT_MAX = 50
_CLEAR_DEFAULT_MAX = 200
# Canonical names of admin/system entries in user_base that must never
# be pruned even if they don't have a corresponding beanie record.
_PROTECTED_USER_DNS = {'admin'}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _build_user_record(user: User) -> LDAPUserRecord:
    keys = await SshKey.find(SshKey.user.id == user.id).to_list()
    return LDAPUserRecord(
        username=user.name,
        email=user.email,
        uid=user.uid,
        gid=user.gid,
        fullname=user.fullname,
        surname=(
            user.surname if user.surname is not None
            else user.fullname.split()[-1]
        ),
        home_directory=user.home_directory,
        shell=user.shell,
        ssh_keys=[k.key for k in keys],
        password=user.password,
        expires_at=user.expires_at,
    )


# ---------------------------------------------------------------------------
# BootstrapLDAPSite
# ---------------------------------------------------------------------------


class BootstrapLDAPSite(Operation):
    """Create the per-site LDAP tree and the special access/status groups.

    Idempotent: re-running reports `already_exists` for things that were
    already there. Fails fast if no AccessGroup/StatusGroup records exist
    in beanie (operator should run SeedAccessStatusGroups first).
    """

    op_name = 'bootstrap_ldap_site'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        ldap: AsyncLDAPManager,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.ldap = ldap
        self._tree_result: dict[str, str] = {}
        self._special_result: dict[str, str] = {}

    async def execute(
        self, session: AsyncClientSession,
    ) -> dict[str, dict[str, str]]:
        access_groups = await AccessGroup.find_all().to_list()
        status_groups = await StatusGroup.find_all().to_list()
        if not access_groups and not status_groups:
            raise ValueError(
                'No AccessGroup / StatusGroup records in beanie; run '
                'SeedAccessStatusGroups first'
            )

        self._tree_result = await self.ldap.ensure_site_tree()

        records = [
            LDAPGroupRecord(groupname=g.name, gid=g.gid)
            for g in itertools.chain(access_groups, status_groups)
        ]
        self._special_result = await self.ldap.ensure_special_groups(records)

        return {'tree': self._tree_result, 'special_groups': self._special_result}

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'tree_count': len(self._tree_result),
            'tree_created': sum(
                1 for v in self._tree_result.values() if v == 'created'
            ),
            'special_groups_count': len(self._special_result),
            'special_groups_created': sum(
                1 for v in self._special_result.values() if v == 'created'
            ),
        }


# ---------------------------------------------------------------------------
# SyncUserToLDAP / SyncGroupToLDAP
# ---------------------------------------------------------------------------


@dataclass
class LDAPSyncResult:
    """One result type for both user and group sync ops. The driver only
    reads `.outcome` for tally bucketing; `name` and `extra` are for ad-hoc
    operator inspection."""

    name: str
    outcome: str
    extra: dict[str, Any]


class SyncUserToLDAP(Operation):
    """Project one beanie User to LDAP.

    Upserts the user dn (delete-and-recreate when `force=True`) and
    reconciles access/status special-group memberships against the targets
    computed from `effective_access_links(user, usi)` plus `user.status`.
    The membership patch uses per-group add/remove to avoid clobbering
    memberships from other sites that share the same special group.

    Outcomes: `created`, `updated`, `recreated`, `no_op`.
    """

    op_name = 'sync_user_to_ldap'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        username: str,
        sitename: str,
        ldap: AsyncLDAPManager,
        force: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.username = username
        self.sitename = sitename
        self.ldap = ldap
        self.force = force
        self._outcome: str = ''
        self._added_groups: list[str] = []
        self._removed_groups: list[str] = []

    async def execute(
        self, session: AsyncClientSession,
    ) -> LDAPSyncResult:
        user = await User.find_one(User.name == self.username)
        if user is None:
            raise ValueError(f'User {self.username!r} does not exist')

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        usi = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
            UserSiteInfo.site.id == site.id,
        )
        if usi is None:
            raise ValueError(
                f'User {self.username!r} is not on site {self.sitename!r}'
            )

        access_links = effective_access_links(user, usi)
        target_groups = set(await resolve_access_ldapnames(access_links))
        status_ldapname = await resolve_status_ldapname(user.status)
        if status_ldapname is not None:
            target_groups.add(status_ldapname)

        record = await _build_user_record(user)
        outcome = await self._upsert_user(record)
        await self._reconcile_memberships(target_groups)

        if (
            outcome == 'updated'
            and not self._added_groups
            and not self._removed_groups
        ):
            outcome = 'no_op'
        self._outcome = outcome

        return LDAPSyncResult(
            name=self.username, outcome=outcome,
            extra={
                'dn': self.ldap.user_dn(self.username),
                'added_groups': list(self._added_groups),
                'removed_groups': list(self._removed_groups),
            },
        )

    async def _upsert_user(self, record: LDAPUserRecord) -> str:
        if self.force:
            await self.ldap.delete_user(self.username)
            await self.ldap.add_user(record)
            return 'recreated'

        # Try update first; fall back to add on miss. Saves the existence
        # probe round-trip per user across the driver.
        try:
            await self.ldap.update_user(record)
            return 'updated'
        except LDAPNotFound:
            await self.ldap.add_user(record)
            return 'created'

    async def _reconcile_memberships(self, target_groups: set[str]) -> None:
        current = await self.ldap.list_user_memberships(self.username)
        to_add = target_groups - current
        to_remove = current - target_groups
        for g in sorted(to_add):
            try:
                await self.ldap.add_users_to_group(
                    g, [self.username], verify_users=False,
                )
                self._added_groups.append(g)
            except LDAPNotFound:
                logger.warning(
                    'sync_user_to_ldap(%s): target group %s not in LDAP; '
                    'run BootstrapLDAPSite first', self.username, g,
                )
        for g in sorted(to_remove):
            try:
                await self.ldap.remove_users_from_group(g, [self.username])
                self._removed_groups.append(g)
            except LDAPNotFound:
                pass

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.username,
            'sitename': self.sitename,
            'outcome': self._outcome,
            'force': self.force,
            'added_groups': list(self._added_groups),
            'removed_groups': list(self._removed_groups),
        }


class SyncGroupToLDAP(Operation):
    """Project one beanie Group (regular, NOT AccessGroup/StatusGroup) to
    LDAP at one site. Members are filtered to those with a UserSiteInfo
    for (member, site).

    Skips access/status groups — those are reconciled by `SyncUserToLDAP`.
    """

    op_name = 'sync_group_to_ldap'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        groupname: str,
        sitename: str,
        ldap: AsyncLDAPManager,
        force: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.groupname = groupname
        self.sitename = sitename
        self.ldap = ldap
        self.force = force
        self._outcome: str = ''
        self._member_count: int = 0

    async def execute(
        self, session: AsyncClientSession,
    ) -> LDAPSyncResult:
        group = await Group.find_one(
            Group.name == self.groupname,
            with_children=True,
            fetch_links=True,
            nesting_depth=1,
        )
        if group is None:
            raise ValueError(f'Group {self.groupname!r} does not exist')

        if isinstance(group, (AccessGroup, StatusGroup)):
            self._outcome = 'skipped_special'
            return LDAPSyncResult(
                name=self.groupname, outcome='skipped_special',
                extra={'member_count': 0},
            )

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        member_names = await self._members_at_site(group, site)

        if not member_names and not self.force:
            self._outcome = 'skipped_no_members_on_site'
            return LDAPSyncResult(
                name=self.groupname, outcome=self._outcome,
                extra={'member_count': 0},
            )

        if self.force:
            await self.ldap.delete_group(self.groupname)

        # Try membership-diff first; fall back to add on miss. Saves the
        # group_exists probe round-trip.
        record = LDAPGroupRecord(
            groupname=self.groupname, gid=group.gid, members=member_names,
        )
        try:
            await self.ldap.set_group_members(self.groupname, member_names)
            self._outcome = 'membership_diffed'
        except LDAPNotFound:
            await self.ldap.add_group(record)
            self._outcome = 'created'
        self._member_count = len(member_names)

        return LDAPSyncResult(
            name=self.groupname, outcome=self._outcome,
            extra={'member_count': self._member_count},
        )

    async def _members_at_site(self, group: Group, site: Site) -> set[str]:
        """Members of `group` that have a UserSiteInfo for `site`."""
        users: dict[Any, User] = {}
        missing_ids: list[Any] = []
        for link in group.members:
            if isinstance(link, User):
                users[link.id] = link
            else:
                missing_ids.append(link.ref.id)
        if missing_ids:
            for u in await User.find(In(User.id, missing_ids)).to_list():
                users[u.id] = u
        if not users:
            return set()
        usis = await UserSiteInfo.find(
            In(UserSiteInfo.user.id, list(users)),
            UserSiteInfo.site.id == site.id,
        ).to_list()
        present = {usi.user.ref.id for usi in usis}
        return {users[uid].name for uid in present if uid in users}

    def describe(self) -> dict[str, Any]:
        return {
            'groupname': self.groupname,
            'sitename': self.sitename,
            'outcome': self._outcome,
            'member_count': self._member_count,
            'force': self.force,
        }


# ---------------------------------------------------------------------------
# SyncSiteAutomounts
# ---------------------------------------------------------------------------


class SyncSiteAutomounts(Operation):
    """Refresh home/group automount entries for a site from beanie Storage
    rows. Uses upsert (delete + add per entry) which is safer than v1's
    wipe-the-whole-map approach when storage rows are partially inconsistent.
    """

    op_name = 'sync_site_automounts'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        ldap: AsyncLDAPManager,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.ldap = ldap
        self._counts: dict[str, int] = {'home_automounts': 0, 'group_automounts': 0}

    async def execute(self, session: AsyncClientSession) -> dict[str, int]:
        from ..queries.storage import list_automap_storages

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        for category, mapname, tally_key in (
            ('home', AUTO_HOME, 'home_automounts'),
            ('group', AUTO_GROUP, 'group_automounts'),
        ):
            for storage in await list_automap_storages(site, category):
                await self.ldap.upsert_automount(
                    mountname=storage.mount_name or storage.name,
                    mapname=mapname,
                    host=storage.host,
                    path=storage.host_path,
                    options=_render_mount_options(storage),
                )
                self._counts[tally_key] += 1
        return dict(self._counts)

    def describe(self) -> dict[str, Any]:
        return {'sitename': self.sitename, **self._counts}


def _render_mount_options(storage: Storage) -> str:
    """`-opt1,opt2` token expected by automountInformation, or '' when no
    options are configured."""
    if not storage.mount_options:
        return ''
    return f'-{",".join(storage.mount_options)}'


# ---------------------------------------------------------------------------
# ClearLDAPTree
# ---------------------------------------------------------------------------


class ClearLDAPTree(Operation):
    """Delete every LDAP entry under a base (default: configured
    searchbase), excluding any subtree listed in `exclude_bases`.

    The LDAP-side analogue of `cheeto ng migrate --drop`: wipe the
    directory so a subsequent `bootstrap` + `sync-site` starts clean.
    The Services OU is excluded by default since cheeto doesn't manage
    it.

    Safety guards mirror PruneSiteLDAP: `max_deletions` cap (default
    `_CLEAR_DEFAULT_MAX`, pass `None` to disable), `dry_run` preview
    that returns the would-delete list without writing.
    """

    op_name = 'clear_ldap_tree'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        ldap: AsyncLDAPManager,
        base: str | None = None,
        exclude_bases: list[str] | None = None,
        max_deletions: int | None = _CLEAR_DEFAULT_MAX,
        dry_run: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.ldap = ldap
        self.base = base or ldap.config.searchbase
        # Default exclusion: the Services OU (service accounts, replication
        # agents, etc. — cheeto doesn't manage these).
        self.exclude_bases = (
            list(exclude_bases) if exclude_bases is not None
            else [f'ou=Services,{ldap.config.searchbase}']
        )
        self.max_deletions = max_deletions
        self.dry_run = dry_run
        self._deleted: list[str] = []

    async def execute(
        self, session: AsyncClientSession,
    ) -> dict[str, Any]:
        dns = await self.ldap.list_subtree_dns(
            self.base, exclude_bases=self.exclude_bases,
        )
        self.logger.info(
            'ClearLDAPTree planning: base=%s exclude=%s candidates=%d',
            self.base, self.exclude_bases, len(dns),
        )

        if (
            self.max_deletions is not None
            and len(dns) > self.max_deletions
        ):
            raise LDAPPruneAborted(
                f'ClearLDAPTree would delete {len(dns)} entries, exceeding '
                f'cap of {self.max_deletions}. Pass max_deletions=None '
                f'(or --max-deletions=-1 on the CLI) to disable.',
                would_delete={'tree': dns},
            )

        if self.dry_run:
            self.logger.info(
                'ClearLDAPTree dry-run: %d DNs would be deleted', len(dns),
            )
            return {'would_delete': dns, 'count': len(dns), 'dry_run': True}

        # DNs in `dns` are already leaf-first (sorted by depth desc).
        # Same-depth nodes are independent — delete each depth-band in
        # parallel via gather, but keep deeper bands ahead of shallower
        # so we never orphan a parent OU.
        for _depth, batch in itertools.groupby(dns, key=lambda d: d.count(',')):
            batch_dns = list(batch)
            results = await asyncio.gather(*(
                self.ldap.delete_dn(dn) for dn in batch_dns
            ))
            self._deleted.extend(
                dn for dn, deleted in zip(batch_dns, results) if deleted
            )
        self.logger.info(
            'ClearLDAPTree done: %d DNs deleted', len(self._deleted),
        )
        return {'deleted': self._deleted, 'count': len(self._deleted)}

    def describe(self) -> dict[str, Any]:
        return {
            'base': self.base,
            'exclude_bases': list(self.exclude_bases),
            'dry_run': self.dry_run,
            'max_deletions': self.max_deletions,
            'deleted_count': len(self._deleted),
        }


# ---------------------------------------------------------------------------
# PruneSiteLDAP
# ---------------------------------------------------------------------------


class PruneSiteLDAP(Operation):
    """Delete LDAP entries that have no corresponding beanie record.

    Three phases (`scope` controls which run):
      - users: ldap users not in beanie's User collection
      - groups: ldap groups (under groups_ou) not in beanie's groups
        collection. AccessGroup/StatusGroup names are NEVER pruned.
      - automounts: automountKey values in auto.home/auto.group not
        in beanie's Storage rows.

    `max_deletions` caps total deletions across phases; passing it as None
    disables the cap. When the cap would be exceeded, raises
    `LDAPPruneAborted` with the would-delete list before any mutations.

    `dry_run=True` returns what would be deleted without writing.
    """

    op_name = 'prune_site_ldap'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        ldap: AsyncLDAPManager,
        scope: list[str] | None = None,
        max_deletions: int | None = _PRUNE_DEFAULT_MAX,
        dry_run: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.ldap = ldap
        self.scope = scope if scope is not None else [
            'users', 'groups', 'automounts',
        ]
        self.max_deletions = max_deletions
        self.dry_run = dry_run
        self._deleted: dict[str, list[str]] = {
            'users': [], 'groups': [], 'automounts': [],
        }

    async def execute(
        self, session: AsyncClientSession,
    ) -> dict[str, list[str]]:
        plan = await self._plan()

        total = sum(len(v) for v in plan.values())
        if (
            self.max_deletions is not None
            and total > self.max_deletions
        ):
            raise LDAPPruneAborted(
                f'Prune would delete {total} entries, exceeding cap of '
                f'{self.max_deletions}. Pass --max-deletions=-1 to disable '
                f'the cap, or narrow --scope.',
                would_delete=plan,
            )

        if self.dry_run:
            self._deleted = plan
            return plan

        for username in plan['users']:
            await self.ldap.delete_user(username)
            self._deleted['users'].append(username)
        for groupname in plan['groups']:
            await self.ldap.delete_group(groupname)
            self._deleted['groups'].append(groupname)
        for entry in plan['automounts']:
            mapname, key = entry.split(':', 1)
            await self.ldap.delete_automount(key, mapname)
            self._deleted['automounts'].append(entry)
        return self._deleted

    async def _plan(self) -> dict[str, list[str]]:
        plan: dict[str, list[str]] = {
            'users': [], 'groups': [], 'automounts': [],
        }
        if 'users' in self.scope:
            plan['users'] = await self._plan_users()
        if 'groups' in self.scope:
            plan['groups'] = await self._plan_groups()
        if 'automounts' in self.scope:
            plan['automounts'] = await self._plan_automounts()
        return plan

    async def _plan_users(self) -> list[str]:
        ldap_users = await self.ldap.list_users()
        beanie_names = {u.name for u in await User.find_all().to_list()}
        return [
            u.username for u in ldap_users
            if u.username not in _PROTECTED_USER_DNS
            and u.username not in beanie_names
        ]

    async def _plan_groups(self) -> list[str]:
        ldap_groups = await self.ldap.list_groups()
        beanie_names = {
            g.name for g in
            await Group.find_all(with_children=True).to_list()
        }
        access_names = {
            ag.name for ag in await AccessGroup.find_all().to_list()
        }
        status_names = {
            sg.name for sg in await StatusGroup.find_all().to_list()
        }
        protected = access_names | status_names
        return [
            g.groupname for g in ldap_groups
            if g.groupname not in protected
            and g.groupname not in beanie_names
        ]

    async def _plan_automounts(self) -> list[str]:
        from ..queries.storage import list_automap_storages
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        out: list[str] = []
        for category, mapname in (('home', AUTO_HOME), ('group', AUTO_GROUP)):
            ldap_keys = await self.ldap.list_automounts(mapname)
            expected = {
                s.mount_name or s.name
                for s in await list_automap_storages(site, category)
            }
            out.extend(f'{mapname}:{k}' for k in ldap_keys if k not in expected)
        return out

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'scope': list(self.scope),
            'dry_run': self.dry_run,
            'max_deletions': self.max_deletions,
            'deleted_count': sum(len(v) for v in self._deleted.values()),
            'deleted_users': len(self._deleted['users']),
            'deleted_groups': len(self._deleted['groups']),
            'deleted_automounts': len(self._deleted['automounts']),
        }


# ---------------------------------------------------------------------------
# SyncSiteLDAP — driver
# ---------------------------------------------------------------------------


_SYNC_TALLY_KEYS = (
    'created', 'updated', 'recreated', 'memberships_only', 'no_op',
    'membership_diffed', 'skipped_special', 'skipped_no_members_on_site',
    'skipped_inactive', 'transient_error', 'error',
)


class SyncSiteLDAP(Operation):
    """Driver: full-site sync.

    1. SyncUserToLDAP per User on the site (UserSiteInfo present).
    2. SyncGroupToLDAP per Group with at least one site member.
    3. SyncSiteAutomounts.
    4. PruneSiteLDAP (unless prune=False).

    Each inner .run() opens its own session/transaction (Operation base
    behavior); a single failure does not cascade. Transient errors are
    counted as 'transient_error' and skipped.

    `scope` restricts to subsets of
    `['users', 'groups', 'automounts', 'prune']`.
    """

    op_name = 'sync_site_ldap'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
        ldap: AsyncLDAPManager,
        force: bool = False,
        concurrency: int = 1,
        scope: list[str] | None = None,
        prune: bool = True,
        max_deletions: int | None = _PRUNE_DEFAULT_MAX,
        dry_run: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self.ldap = ldap
        self.force = force
        self.concurrency = max(1, concurrency)
        self.scope = scope if scope is not None else [
            'users', 'groups', 'automounts', 'prune',
        ]
        self.prune = prune
        self.max_deletions = max_deletions
        self.dry_run = dry_run
        self._users_tally: dict[str, int] = {k: 0 for k in _SYNC_TALLY_KEYS}
        self._groups_tally: dict[str, int] = {k: 0 for k in _SYNC_TALLY_KEYS}
        self._automounts_result: dict[str, int] = {}
        self._prune_result: dict[str, Any] = {}

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        if 'users' in self.scope:
            usis = await UserSiteInfo.find(
                UserSiteInfo.site.id == site.id,
                fetch_links=True, nesting_depth=1,
            ).to_list()
            usernames = [usi.user.name for usi in usis if usi.user is not None]
            await self._run_per_record(
                self._users_tally, 'user', usernames,
                lambda name: SyncUserToLDAP.run(
                    self.client, self.author,
                    username=name, sitename=self.sitename,
                    ldap=self.ldap, force=self.force,
                ),
            )

        if 'groups' in self.scope:
            # Group.find_all() against the base class is polymorphic-aware:
            # subclasses (AccessGroup/StatusGroup) are filtered by _class_id
            # when with_children is omitted.
            groups = await Group.find_all().to_list()
            group_names = [g.name for g in groups]
            await self._run_per_record(
                self._groups_tally, 'group', group_names,
                lambda name: SyncGroupToLDAP.run(
                    self.client, self.author,
                    groupname=name, sitename=self.sitename,
                    ldap=self.ldap, force=self.force,
                ),
            )

        if 'automounts' in self.scope:
            await self._sync_automounts()
        if 'prune' in self.scope and self.prune:
            await self._prune()

        return {
            'users': dict(self._users_tally),
            'groups': dict(self._groups_tally),
            'automounts': dict(self._automounts_result),
            'pruned': dict(self._prune_result),
        }

    async def _run_per_record(
        self,
        tally: dict[str, int],
        label: str,
        names: list[str],
        run_op: Callable[[str], Awaitable[LDAPSyncResult]],
    ) -> None:
        """Run `run_op(name)` for each name, bucketing the result outcome
        into `tally`. Transient errors and unexpected exceptions are caught
        per-record (one failure does not cascade)."""

        async def _one(name: str) -> None:
            try:
                result = await run_op(name)
            except LDAPTransientError as e:
                logger.warning(
                    'LDAP transient error for %s %s: %s', label, name, e,
                )
                tally['transient_error'] += 1
                return
            except Exception as e:
                logger.exception(
                    'LDAP %s sync failed for %s: %s', label, name, e,
                )
                tally['error'] += 1
                return
            tally.setdefault(result.outcome, 0)
            tally[result.outcome] += 1

        if self.concurrency == 1 or label == 'group':
            # Groups stay serial — set_group_members order-sensitive within
            # the same site. Users fan out via the connection pool.
            for name in names:
                await _one(name)
            return

        sem = asyncio.Semaphore(self.concurrency)

        async def _bounded(name: str) -> None:
            async with sem:
                await _one(name)

        await asyncio.gather(*(_bounded(n) for n in names))

    async def _sync_automounts(self) -> None:
        try:
            self._automounts_result = await SyncSiteAutomounts.run(
                self.client, self.author,
                sitename=self.sitename, ldap=self.ldap,
            )
        except Exception as e:
            logger.exception('LDAP automount sync failed: %s', e)
            self._automounts_result = {'error': str(e)}

    async def _prune(self) -> None:
        try:
            self._prune_result = await PruneSiteLDAP.run(
                self.client, self.author,
                sitename=self.sitename, ldap=self.ldap,
                max_deletions=self.max_deletions, dry_run=self.dry_run,
            )
        except LDAPPruneAborted as e:
            logger.warning('Prune aborted: %s', e)
            self._prune_result = {
                'aborted': True,
                'would_delete': e.would_delete,
            }
        except Exception as e:
            logger.exception('Prune failed: %s', e)
            self._prune_result = {'error': str(e)}

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'force': self.force,
            'concurrency': self.concurrency,
            'scope': list(self.scope),
            'prune': self.prune,
            'dry_run': self.dry_run,
            'users_tally': dict(self._users_tally),
            'groups_tally': dict(self._groups_tally),
            'automounts_result': dict(self._automounts_result),
            'prune_result': {
                k: (len(v) if isinstance(v, list) else v)
                for k, v in self._prune_result.items()
            },
        }
