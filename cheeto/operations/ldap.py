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
import logging
from dataclasses import dataclass
from typing import Any

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..ldap_async import (
    AsyncLDAPManager,
    LDAPAutomountRecord,
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
from .base import Operation

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers shared across ops
# ---------------------------------------------------------------------------


async def _resolve_user_status_name(user: User) -> str | None:
    """Resolve User.status (a Link[StatusGroup] or None) to its
    status_name string. Avoids requiring fetch_links= at every call site."""
    if user.status is None:
        return None
    if isinstance(user.status, StatusGroup):
        return user.status.status_name
    sg = await StatusGroup.get(user.status.ref.id)
    return sg.status_name if sg is not None else None


async def _resolve_user_status_ldapname(user: User) -> str | None:
    """Resolve User.status to its LDAP groupname (Group.name)."""
    if user.status is None:
        return None
    if isinstance(user.status, StatusGroup):
        return user.status.name
    sg = await StatusGroup.get(user.status.ref.id)
    return sg.name if sg is not None else None


async def _resolve_access_ldapnames(
    access_links: list, /,
) -> list[str]:
    """Resolve a list of Link[AccessGroup] (or fetched AccessGroup docs)
    into their LDAP groupnames."""
    out: list[str] = []
    for link in access_links:
        if isinstance(link, AccessGroup):
            out.append(link.name)
            continue
        ag = await AccessGroup.get(link.ref.id)
        if ag is not None:
            out.append(ag.name)
    return out


async def _build_user_record(user: User) -> LDAPUserRecord:
    """Construct an LDAPUserRecord from a beanie User. Pulls associated
    SshKey records for ssh_keys."""
    keys = await SshKey.find(SshKey.user.id == user.id).to_list()
    return LDAPUserRecord(
        username=user.name,
        email=user.email,
        uid=user.uid,
        gid=user.gid,
        fullname=user.fullname,
        home_directory=user.home_directory,
        shell=user.shell,
        ssh_keys=[k.key for k in keys],
        password=user.password,
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
        # Verify seed step has run.
        access_groups = await AccessGroup.find_all().to_list()
        status_groups = await StatusGroup.find_all().to_list()
        if not access_groups and not status_groups:
            raise ValueError(
                'No AccessGroup / StatusGroup records in beanie; run '
                'SeedAccessStatusGroups first'
            )

        # 1) Site OU tree + automount maps.
        self._tree_result = await self.ldap.ensure_site_tree()

        # 2) Special-group LDAP entries from the beanie records.
        records: list[LDAPGroupRecord] = []
        for ag in access_groups:
            records.append(LDAPGroupRecord(groupname=ag.name, gid=ag.gid))
        for sg in status_groups:
            records.append(LDAPGroupRecord(groupname=sg.name, gid=sg.gid))
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
# SyncUserToLDAP
# ---------------------------------------------------------------------------


@dataclass
class SyncUserToLDAPResult:
    username: str
    outcome: str
    dn: str
    added_groups: list[str]
    removed_groups: list[str]


class SyncUserToLDAP(Operation):
    """Project one beanie User to LDAP.

    Upserts the user dn (delete-and-recreate when `force=True`) and
    reconciles access/status special-group memberships against the
    targets computed from `User.access | UserSiteInfo.access` plus
    `User.status`. The membership patch uses
    `add_users_to_group` / `remove_users_from_group` per group rather
    than `set_group_members` to avoid clobbering memberships from other
    sites that share the same special group.

    Outcomes:
      - `created` — the user dn did not exist before this run
      - `updated` — the user dn was patched in place
      - `recreated` — `force=True` deleted-then-added
      - `memberships_only` — the dn already matched; only groups changed
      - `no_op` — nothing to do (user already in target state)
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
    ) -> SyncUserToLDAPResult:
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

        # Compute target group memberships.
        access_links = list(user.access) + list(usi.access)
        target_groups = set(await _resolve_access_ldapnames(access_links))
        status_ldapname = await _resolve_user_status_ldapname(user)
        if status_ldapname is not None:
            target_groups.add(status_ldapname)

        # Upsert user dn.
        record = await _build_user_record(user)

        if self.force:
            await self.ldap.delete_user(self.username)

        existed_before = await self.ldap.user_exists(self.username)
        dn = self.ldap.user_dn(self.username)

        if not existed_before:
            await self.ldap.add_user(record)
            self._outcome = 'recreated' if self.force else 'created'
        else:
            await self.ldap.update_user(self.username, **{
                'email': record.email,
                'uid': record.uid,
                'gid': record.gid,
                'fullname': record.fullname,
                'home_directory': record.home_directory,
                'shell': record.shell,
                'ssh_keys': record.ssh_keys,
                'password': record.password,
            })
            self._outcome = 'updated'

        # Reconcile group memberships.
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

        if (
            self._outcome == 'updated'
            and not self._added_groups
            and not self._removed_groups
        ):
            self._outcome = 'no_op'
        elif (
            self._outcome == 'updated'
            and (self._added_groups or self._removed_groups)
        ):
            # Distinguish "fields and groups changed" from "only groups did";
            # we don't compare field-level so just leave as 'updated'.
            pass

        return SyncUserToLDAPResult(
            username=self.username,
            outcome=self._outcome,
            dn=dn,
            added_groups=list(self._added_groups),
            removed_groups=list(self._removed_groups),
        )

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.username,
            'sitename': self.sitename,
            'outcome': self._outcome,
            'force': self.force,
            'added_groups': list(self._added_groups),
            'removed_groups': list(self._removed_groups),
        }


# ---------------------------------------------------------------------------
# SyncGroupToLDAP
# ---------------------------------------------------------------------------


@dataclass
class SyncGroupToLDAPResult:
    groupname: str
    outcome: str
    member_count: int


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
    ) -> SyncGroupToLDAPResult:
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
            return SyncGroupToLDAPResult(
                groupname=self.groupname, outcome='skipped_special',
                member_count=0,
            )

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        # Filter members to those with a UserSiteInfo for this site.
        member_names: set[str] = set()
        for member_link in group.members:
            member = (
                member_link if isinstance(member_link, User)
                else await User.get(member_link.ref.id)
            )
            if member is None:
                continue
            usi = await UserSiteInfo.find_one(
                UserSiteInfo.user.id == member.id,
                UserSiteInfo.site.id == site.id,
            )
            if usi is not None:
                member_names.add(member.name)

        if not member_names and not self.force:
            self._outcome = 'skipped_no_members_on_site'
            return SyncGroupToLDAPResult(
                groupname=self.groupname,
                outcome='skipped_no_members_on_site', member_count=0,
            )

        if self.force:
            await self.ldap.delete_group(self.groupname)

        if not await self.ldap.group_exists(self.groupname):
            await self.ldap.add_group(LDAPGroupRecord(
                groupname=self.groupname, gid=group.gid, members=member_names,
            ))
            self._outcome = 'created'
        else:
            await self.ldap.set_group_members(self.groupname, member_names)
            self._outcome = 'membership_diffed'
        self._member_count = len(member_names)

        return SyncGroupToLDAPResult(
            groupname=self.groupname, outcome=self._outcome,
            member_count=self._member_count,
        )

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
        self._home_count: int = 0
        self._group_count: int = 0

    async def execute(self, session: AsyncClientSession) -> dict[str, int]:
        from ..queries.storage import list_automap_storages

        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        for storage in await list_automap_storages(site, 'home'):
            options = (
                ','.join(storage.mount_options)
                if storage.mount_options else ''
            )
            opt_token = f'-{options}' if options else ''
            await self.ldap.upsert_home_automount(
                username=storage.mount_name or storage.name,
                host=storage.host,
                path=storage.host_path,
                options=opt_token,
            )
            self._home_count += 1

        for storage in await list_automap_storages(site, 'group'):
            options = (
                ','.join(storage.mount_options)
                if storage.mount_options else ''
            )
            opt_token = f'-{options}' if options else ''
            await self.ldap.upsert_group_automount(
                storagename=storage.mount_name or storage.name,
                host=storage.host,
                path=storage.host_path,
                options=opt_token,
            )
            self._group_count += 1

        return {
            'home_automounts': self._home_count,
            'group_automounts': self._group_count,
        }

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'home_automounts': self._home_count,
            'group_automounts': self._group_count,
        }


# ---------------------------------------------------------------------------
# PruneSiteLDAP
# ---------------------------------------------------------------------------


_PRUNE_DEFAULT_MAX = 50
_PROTECTED_USER_DNS = {'admin'}  # canonical names of admin/system entries
                                 # in user_base that must never be pruned


class PruneSiteLDAP(Operation):
    """Delete LDAP entries that have no corresponding beanie record.

    Three phases (`scope` controls which run):
      - users: ldap users not in beanie's User collection
      - groups: ldap groups (under groups_ou) not in beanie's groups
        collection. AccessGroup/StatusGroup names are NEVER pruned.
      - automounts: automountKey values in auto.home/auto.group not
        in beanie's Storage rows.

    `max_deletions` caps total deletions across phases; passing it as
    None disables the cap (operator confirms with eyes open). When the
    cap would be exceeded, raises `LDAPPruneAborted` with the would-delete
    list before any mutations.

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
        # Compute the would-delete sets up front.
        plan: dict[str, list[str]] = {
            'users': [], 'groups': [], 'automounts': [],
        }

        if 'users' in self.scope:
            ldap_users = await self.ldap.list_users()
            beanie_names = {
                u.name for u in await User.find_all().to_list()
            }
            for u in ldap_users:
                if u.username in _PROTECTED_USER_DNS:
                    continue
                if u.username not in beanie_names:
                    plan['users'].append(u.username)

        if 'groups' in self.scope:
            ldap_groups = await self.ldap.list_groups()
            beanie_groups = await Group.find_all(with_children=True).to_list()
            beanie_group_names = {g.name for g in beanie_groups}
            access_names = {
                ag.name for ag in await AccessGroup.find_all().to_list()
            }
            status_names = {
                sg.name for sg in await StatusGroup.find_all().to_list()
            }
            protected = access_names | status_names
            for g in ldap_groups:
                if g.groupname in protected:
                    continue
                if g.groupname not in beanie_group_names:
                    plan['groups'].append(g.groupname)

        if 'automounts' in self.scope:
            from ..queries.storage import list_automap_storages
            site = await Site.find_one(Site.name == self.sitename)
            if site is None:
                raise ValueError(f'Site {self.sitename!r} does not exist')

            for category, mapname in [('home', 'auto.home'),
                                      ('group', 'auto.group')]:
                ldap_keys = await self.ldap.list_automounts(mapname)
                beanie_storages = await list_automap_storages(site, category)
                expected = {
                    s.mount_name or s.name for s in beanie_storages
                }
                for k in ldap_keys:
                    if k not in expected:
                        plan['automounts'].append(f'{mapname}:{k}')

        # Safety cap.
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

        # Execute phase: delete each entry, populating self._deleted with
        # successes so partial failures still produce an audit trail.
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
        self._prune_result: dict[str, list[str]] = {}

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        if 'users' in self.scope:
            await self._sync_users(site)
        if 'groups' in self.scope:
            await self._sync_groups(site)
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

    async def _sync_users(self, site: Site) -> None:
        usis = await UserSiteInfo.find(
            UserSiteInfo.site.id == site.id,
            fetch_links=True, nesting_depth=1,
        ).to_list()
        usernames = [usi.user.name for usi in usis if usi.user is not None]

        async def _one(name: str) -> None:
            try:
                result = await SyncUserToLDAP.run(
                    self.client, self.author,
                    username=name, sitename=self.sitename,
                    ldap=self.ldap, force=self.force,
                )
            except LDAPTransientError as e:
                logger.warning('LDAP transient error for %s: %s', name, e)
                self._users_tally['transient_error'] += 1
                return
            except Exception as e:
                logger.exception('LDAP user sync failed for %s: %s', name, e)
                self._users_tally['error'] += 1
                return
            self._users_tally.setdefault(result.outcome, 0)
            self._users_tally[result.outcome] += 1

        if self.concurrency == 1:
            for name in usernames:
                await _one(name)
        else:
            sem = asyncio.Semaphore(self.concurrency)

            async def _bounded(n):
                async with sem:
                    await _one(n)

            await asyncio.gather(*(_bounded(n) for n in usernames))

    async def _sync_groups(self, site: Site) -> None:
        # Only regular Groups, not AccessGroup/StatusGroup. Polymorphic
        # find_all() against the base Group class returns ONLY base-class
        # rows (subclasses are filtered by _class_id) when with_children=False.
        groups = await Group.find_all().to_list()
        for group in groups:
            try:
                result = await SyncGroupToLDAP.run(
                    self.client, self.author,
                    groupname=group.name, sitename=self.sitename,
                    ldap=self.ldap, force=self.force,
                )
            except LDAPTransientError as e:
                logger.warning(
                    'LDAP transient error for group %s: %s', group.name, e,
                )
                self._groups_tally['transient_error'] += 1
                continue
            except Exception as e:
                logger.exception(
                    'LDAP group sync failed for %s: %s', group.name, e,
                )
                self._groups_tally['error'] += 1
                continue
            self._groups_tally.setdefault(result.outcome, 0)
            self._groups_tally[result.outcome] += 1

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
