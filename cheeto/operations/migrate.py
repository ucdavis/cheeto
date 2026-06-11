"""Migration operations: move data from old mongoengine collections to new beanie models.

These operations read from the old mongoengine documents (GlobalUser, SiteUser,
GlobalGroup, SiteGroup, Site) and create corresponding new beanie documents
(User, UserSiteInfo, Group, Site).

The mongoengine connection must already be established (for reads) and beanie
must be initialized (for writes).
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Any

from beanie import PydanticObjectId
from beanie.operators import In
from mongoengine import DoesNotExist
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..database.group import GlobalGroup, SiteGroup
from ..database.site import Site as OldSite
from ..database.slurm import (
    SiteSlurmAssociation,
    SiteSlurmPartition,
    SiteSlurmQOS,
)
from ..database.storage import (
    Automount as OldAutomount,
    AutomountMap as OldAutomountMap,
    NFSMountSource as OldNFSMountSource,
    NFSSourceCollection as OldNFSSourceCollection,
    Storage as OldStorage,
    ZFSMountSource as OldZFSMountSource,
)
from ..constants import MIN_SPECIAL_GID, STORAGE_CATEGORIES
from ..database.user import GlobalUser, SiteUser
from ..models.base import link_target_id
from ..models.group import Group
from ..models.group_membership import GroupMembership
from ..models.site import Site
from ..models.slurm import (
    SlurmAccount,
    SlurmAccountLimits,
    SlurmAllocation,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
    SlurmTRES,
)
from ..models.group import AccessGroup, StatusGroup
from ..models.storage import (
    AutomountMap,
    MountOverrides,
    NFSExportConfig,
    Storage,
    StorageAllocation,
    StorageVolume,
    ZFSConfig,
)
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo
from .base import Operation


async def _resolve_access_links_for_migration(
    names: list[str],
) -> list[AccessGroup]:
    """Migration variant of access-link resolution that warns and skips
    missing AccessGroup records instead of raising. v1 may have access
    types that didn't get seeded; we don't want one orphaned name to
    abort an entire user migration."""
    out: list[AccessGroup] = []
    for n in names:
        ag = await AccessGroup.find_one(AccessGroup.access_name == n)
        if ag is None:
            logger.warning(
                'No AccessGroup with access_name=%r for migrated user; '
                'skipping that access type', n,
            )
            continue
        out.append(ag)
    return out


async def _resolve_status_link_for_migration(
    name: str | None,
) -> StatusGroup | None:
    if name is None:
        return None
    sg = await StatusGroup.find_one(StatusGroup.status_name == name)
    if sg is None:
        logger.warning(
            'No StatusGroup with status_name=%r for migrated user; '
            'leaving status unset', name,
        )
    return sg

logger = logging.getLogger(__name__)


class MigrateAccessStatusGroups(Operation):
    """Seed beanie AccessGroup/StatusGroup records so MigrateUsers (which
    expects access/status to resolve to Links) can run successfully.

    Always seeds the canonical set in `DEFAULT_ACCESS_GROUPS` /
    `DEFAULT_STATUS_GROUPS` (so types like `slurm` and `ondemand` exist
    even when v1 didn't provision LDAP groups for them). Optional
    `access_groups` / `status_groups` kwargs are *additive* — entries not
    already present in defaults get seeded too. They do not override
    default LDAP names; the v2 schema decouples e.g. `status='active'`
    from `access='login-ssh'` and we don't want a stale v1 mapping
    (`active: login-ssh-users`) to re-introduce that overlap.

    All gids come from `gid_start` upward (default `MIN_SPECIAL_GID`).
    The v1 GlobalGroup gid is intentionally not inherited — special
    groups have no file ownerships, so renumbering them out of the
    SYSTEM_UID range is safe and keeps them in the dedicated band.

    Behavior per (shorthand, ldap_name) pair:
      - existing AccessGroup/StatusGroup with matching shorthand → skip
        ('already_exists'), gid preserved.
      - legacy doc with that LDAP name and `_class_id=None` (predates
        the polymorphic schema redesign) → patch in place: stamp
        `_class_id`, set shorthand + type, **also reassign gid** out of
        the legacy SYSTEM_UID range. Marked 'upgraded'.
      - otherwise → fresh insert with gid from `gid_start`.

    Run BEFORE `MigrateUsers` — users have `Link[AccessGroup]` /
    `Link[StatusGroup]` fields that need these records to exist.
    """

    op_name = 'migrate_access_status_groups'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        access_groups: dict | None = None,
        status_groups: dict | None = None,
        gid_start: int = MIN_SPECIAL_GID,
    ) -> None:
        super().__init__(client, author)
        self.access_groups = access_groups or {}
        self.status_groups = status_groups or {}
        self.gid_start = gid_start
        self._created_access: dict[str, str] = {}
        self._created_status: dict[str, str] = {}

    async def _next_unused_gid(self, used: set[int]) -> int:
        gid = self.gid_start
        while gid in used:
            gid += 1
        used.add(gid)
        return gid

    async def _seed_special_group(
        self,
        model_cls: type,
        shorthand_field: str,
        shorthand: str,
        ldap_name: str,
        type_str: str,
        bucket: dict[str, str],
        used_gids: set[int],
        session: AsyncClientSession,
    ) -> None:
        cls_label = model_cls.__name__
        self.logger.info(
            'Seeding %s %s=%s ldap_name=%s',
            cls_label, shorthand_field, shorthand, ldap_name,
        )
        existing = await model_cls.find_one(
            getattr(model_cls, shorthand_field) == shorthand,
        )
        if existing is not None:
            self.logger.info(
                '%s %s already exists with gid=%d; preserving',
                cls_label, shorthand, existing.gid,
            )
            bucket[shorthand] = 'already_exists'
            used_gids.add(existing.gid)
            return

        # Upgrade-in-place: a legacy doc (from before is_root=True was added
        # to Group) may already occupy this LDAP name. Beanie's polymorphic
        # find returns nothing for it because _class_id is unset, but the
        # unique index on `name` will still reject our insert. Patch the
        # discriminator + shorthand + type in place instead, and pull the
        # gid into the special-gid band — special groups have no file
        # ownerships so renumbering is safe.
        coll = model_cls.get_pymongo_collection()
        legacy = await coll.find_one(
            {'name': ldap_name, '_class_id': None},
            session=session,
        )
        if legacy is not None:
            new_gid = await self._next_unused_gid(used_gids)
            await coll.update_one(
                {'_id': legacy['_id']},
                {'$set': {
                    '_class_id': model_cls._class_id,
                    shorthand_field: shorthand,
                    'type': type_str,
                    'gid': new_gid,
                }},
                session=session,
            )
            self.logger.info(
                '%s %s upgraded in place: legacy %s doc gid %d -> %d '
                '(was type=%r, _class_id=None)',
                cls_label, shorthand, ldap_name, legacy.get('gid'), new_gid,
                legacy.get('type'),
            )
            bucket[shorthand] = 'upgraded'
            return

        gid = await self._next_unused_gid(used_gids)
        self.logger.info(
            '%s %s gid=%d newly allocated', cls_label, shorthand, gid,
        )
        record = model_cls(
            name=ldap_name, gid=gid, type=type_str,
            **{shorthand_field: shorthand},
        )
        await record.insert(session=session)
        bucket[shorthand] = 'created'

    async def execute(
        self, session: AsyncClientSession,
    ) -> dict[str, dict[str, str]]:
        # Defaults are always seeded; kwargs are additive (extras only).
        # Default LDAP names are not overridable here — the v2 schema's
        # status/access decoupling depends on the canonical mapping (e.g.
        # active=active-users, NOT active=login-ssh-users from v1).
        from .group import DEFAULT_ACCESS_GROUPS, DEFAULT_STATUS_GROUPS

        access = dict(DEFAULT_ACCESS_GROUPS)
        access_extras = {
            k: v for k, v in self.access_groups.items() if k not in access
        }
        access.update(access_extras)

        status = dict(DEFAULT_STATUS_GROUPS)
        status_extras = {
            k: v for k, v in self.status_groups.items() if k not in status
        }
        status.update(status_extras)

        self.logger.info(
            'MigrateAccessStatusGroups starting: access=%d (defaults=%d, '
            'extras=%d) status=%d (defaults=%d, extras=%d) gid_start=%d',
            len(access), len(DEFAULT_ACCESS_GROUPS), len(access_extras),
            len(status), len(DEFAULT_STATUS_GROUPS), len(status_extras),
            self.gid_start,
        )

        # Pre-load all gids in use across the polymorphic groups collection
        # so we can avoid duplicate-gid violations when allocating.
        all_groups = await Group.find_all(with_children=True).to_list()
        used_gids: set[int] = {g.gid for g in all_groups}
        self.logger.info(
            '%d gids already in use across the polymorphic groups collection',
            len(used_gids),
        )

        for access_name, ldap_name in access.items():
            await self._seed_special_group(
                AccessGroup, 'access_name',
                access_name, ldap_name, 'access',
                self._created_access, used_gids, session,
            )
        for status_name, ldap_name in status.items():
            await self._seed_special_group(
                StatusGroup, 'status_name',
                status_name, ldap_name, 'status',
                self._created_status, used_gids, session,
            )

        a = self._created_access.values()
        s = self._created_status.values()
        a_created = sum(1 for v in a if v == 'created')
        a_upgraded = sum(1 for v in a if v == 'upgraded')
        a_skipped = sum(1 for v in a if v == 'already_exists')
        s_created = sum(1 for v in s if v == 'created')
        s_upgraded = sum(1 for v in s if v == 'upgraded')
        s_skipped = sum(1 for v in s if v == 'already_exists')
        self.logger.info(
            'MigrateAccessStatusGroups done: access created=%d upgraded=%d '
            'skipped=%d, status created=%d upgraded=%d skipped=%d',
            a_created, a_upgraded, a_skipped,
            s_created, s_upgraded, s_skipped,
        )

        return {
            'access': dict(self._created_access),
            'status': dict(self._created_status),
        }

    def describe(self) -> dict[str, Any]:
        a = self._created_access.values()
        s = self._created_status.values()
        return {
            'access_groups_count': len(self._created_access),
            'access_groups_created': sum(1 for v in a if v == 'created'),
            'access_groups_upgraded': sum(1 for v in a if v == 'upgraded'),
            'status_groups_count': len(self._created_status),
            'status_groups_created': sum(1 for v in s if v == 'created'),
            'status_groups_upgraded': sum(1 for v in s if v == 'upgraded'),
            'gid_start': self.gid_start,
        }


class MigrateSites(Operation):
    op_name = 'migrate_sites'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped = 0
        self.filtered = 0

    async def execute(self, session: AsyncClientSession) -> list[Site]:
        total = OldSite.objects.count()
        self.logger.info(
            'MigrateSites starting: %d v1 sites to consider', total,
        )
        sites = []
        for old_site in OldSite.objects():
            if (
                self.sitenames is not None
                and old_site.sitename not in self.sitenames
            ):
                self.filtered += 1
                continue
            existing = await Site.find_one(Site.name == old_site.sitename)
            if existing is not None:
                self.logger.info(
                    'Site %s already exists, skipping', old_site.sitename,
                )
                self.skipped += 1
                continue

            site = Site(
                name=old_site.sitename,
                fqdn=old_site.fqdn,
            )
            await site.insert(session=session)
            sites.append(site)
            self.migrated += 1
            self.logger.info('Migrated site %s', old_site.sitename)

        self.logger.info(
            'MigrateSites done: migrated=%d skipped=%d filtered=%d',
            self.migrated, self.skipped, self.filtered,
        )
        return sites

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateSiteGlobals(Operation):
    """Fold v1 `Site.global_groups` and `Site.global_slurmers` into the v2
    sticky lists.

    Must run AFTER `MigrateSites`, `MigrateGroups`, and
    `MigrateSlurmAccounts` — both fields point at v2 records.
    """

    op_name = 'migrate_site_globals'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.sites_updated = 0
        self.groups_added = 0
        self.slurmers_added = 0
        self.groups_missing = 0
        self.accounts_missing = 0
        self.filtered = 0

    async def execute(self, session: AsyncClientSession) -> int:
        from ..models.slurm import SlurmAccount

        total = OldSite.objects.count()
        self.logger.info(
            'MigrateSiteGlobals starting: %d v1 sites to consider', total,
        )

        # One-shot preload: the migration is a batch run, so loading every
        # Group + SlurmAccount up-front trades N+1 lookups for a constant.
        groups_by_name: dict[str, Group] = {
            g.name: g
            for g in await Group.find_all(with_children=True).to_list()
        }
        accounts_by_pair: dict[tuple, SlurmAccount] = {
            (link_target_id(a.group), link_target_id(a.site)): a
            for a in await SlurmAccount.find_all().to_list()
        }

        for old_site in OldSite.objects():
            if (
                self.sitenames is not None
                and old_site.sitename not in self.sitenames
            ):
                self.filtered += 1
                continue
            new_site = await Site.find_one(Site.name == old_site.sitename)
            if new_site is None:
                self.logger.warning(
                    'Site %s not in beanie; skipping globals fold',
                    old_site.sitename,
                )
                continue

            # Sticky lists hold bare DocRef ids (models/base.py::DocRef).
            existing_group_ids = set(new_site.group.sticky)
            existing_account_ids = set(new_site.slurm.sticky)
            changed = False

            for old_sg in old_site.global_groups or []:
                grp = groups_by_name.get(old_sg.parent.groupname)
                if grp is None:
                    self.logger.warning(
                        'global_groups[%s] -> %s not in beanie; skipping',
                        old_site.sitename, old_sg.parent.groupname,
                    )
                    self.groups_missing += 1
                    continue
                if grp.id in existing_group_ids:
                    continue
                new_site.group.sticky.append(grp.id)
                existing_group_ids.add(grp.id)
                self.groups_added += 1
                changed = True

            for old_sg in old_site.global_slurmers or []:
                grp = groups_by_name.get(old_sg.parent.groupname)
                if grp is None:
                    self.logger.warning(
                        'global_slurmers[%s] -> group %s not in beanie',
                        old_site.sitename, old_sg.parent.groupname,
                    )
                    self.groups_missing += 1
                    continue
                account = accounts_by_pair.get((grp.id, new_site.id))
                if account is None:
                    self.logger.warning(
                        'global_slurmers[%s] -> no SlurmAccount for group %s',
                        old_site.sitename, old_sg.parent.groupname,
                    )
                    self.accounts_missing += 1
                    continue
                if account.id in existing_account_ids:
                    continue
                new_site.slurm.sticky.append(account.id)
                existing_account_ids.add(account.id)
                self.slurmers_added += 1
                changed = True

            if changed:
                await new_site.save(session=session)
                self.sites_updated += 1
                self.logger.info(
                    'MigrateSiteGlobals: %s updated', old_site.sitename,
                )

        self.logger.info(
            'MigrateSiteGlobals done: sites=%d groups+=%d slurmers+=%d '
            '(missing groups=%d, missing accounts=%d)',
            self.sites_updated, self.groups_added, self.slurmers_added,
            self.groups_missing, self.accounts_missing,
        )
        return self.sites_updated

    def describe(self) -> dict[str, Any]:
        return {
            'sites_updated': self.sites_updated,
            'groups_added': self.groups_added,
            'slurmers_added': self.slurmers_added,
            'groups_missing': self.groups_missing,
            'accounts_missing': self.accounts_missing,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateUser(Operation):
    """Migrate a single user and their site memberships from mongoengine to beanie."""

    op_name = 'migrate_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        username: str,
        sites_by_name: dict[str, Site] | None = None,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.username = username
        self.sitenames = set(sitenames) if sitenames else None
        self.site_infos_created = 0
        self.filtered = 0
        self._sites_by_name = sites_by_name

    async def _resolve_site(self, sitename: str) -> Site | None:
        if self._sites_by_name is not None:
            return self._sites_by_name.get(sitename)
        return await Site.find_one(Site.name == sitename)

    async def execute(self, session: AsyncClientSession) -> User:
        self.logger.info('MigrateUser starting for username=%s', self.username)
        existing = await User.find_one(User.name == self.username)
        if existing is not None:
            raise ValueError(f'User {self.username} already exists in new database')

        old_user = GlobalUser.objects(username=self.username).first()
        if old_user is None:
            raise ValueError(f'User {self.username} not found in old database')

        # Clean-slate access migration: fold the v1 global access list and
        # every v1 per-site access list into the new v2 `user.access`.
        # v2 per-site `UserSiteInfo.access` uses override semantics and
        # stays empty here — operators can add explicit overrides later.
        # Filter BEFORE folding: a deprecated site's per-site access must
        # not shape the migrated global access list.
        old_site_users = list(SiteUser.objects(username=self.username))
        if self.sitenames is not None:
            before_filter = len(old_site_users)
            old_site_users = [
                osu for osu in old_site_users
                if osu.sitename in self.sitenames
            ]
            self.filtered = before_filter - len(old_site_users)
        access_shorthands: set[str] = set(old_user.access or ['login-ssh'])
        sites_with_extras = 0
        for osu in old_site_users:
            if osu._access:
                before = len(access_shorthands)
                access_shorthands |= set(osu._access)
                if len(access_shorthands) > before:
                    sites_with_extras += 1

        user_status = await _resolve_status_link_for_migration(old_user.status)
        user_access = await _resolve_access_links_for_migration(
            sorted(access_shorthands)
        )
        user = User(
            name=old_user.username,
            email=old_user.email,
            uid=old_user.uid,
            gid=old_user.gid,
            fullname=old_user.fullname,
            shell=old_user.shell,
            type=old_user.type,
            status=user_status,
            password=old_user.password,
            access=user_access,
            comments=list(old_user.comments) if old_user.comments else [],
            home_directory=old_user.home_directory,
        )
        await user.insert(session=session)
        self.logger.info(
            'Migrated user %s (uid=%d, status=%s, access=%d, '
            'folded-in from %d site(s) with extras)',
            old_user.username, old_user.uid,
            user_status.status_name if user_status is not None else None,
            len(user_access), sites_with_extras,
        )

        if old_user.ssh_key:
            for k in old_user.ssh_key:
                await SshKey(key=k, user=user).insert(session=session)
            self.logger.info(
                'Migrated %d ssh key(s) for %s',
                len(old_user.ssh_key), self.username,
            )

        # Migrate SiteUser records for this user. We deliberately leave
        # `UserSiteInfo.access` empty — the v1 per-site overrides have
        # been folded into the global v2 list above, and v2 override
        # semantics rely on empty meaning "fall through to global."
        for old_site_user in old_site_users:
            site = await self._resolve_site(old_site_user.sitename)
            if site is None:
                self.logger.warning(
                    'SiteUser %s references non-existent site %s, skipping',
                    self.username, old_site_user.sitename,
                )
                continue

            usi_status = await _resolve_status_link_for_migration(
                old_site_user._status or 'active'
            )
            usi = UserSiteInfo(
                user=user,
                site=site,
                status=usi_status,
                access=[],
            )
            if old_site_user.expiry:
                usi.expires_at = old_site_user.expiry
            await usi.insert(session=session)
            self.site_infos_created += 1
            self.logger.info(
                'Migrated UserSiteInfo for %s on %s',
                self.username, old_site_user.sitename,
            )

        self.logger.info(
            'MigrateUser done for %s: site_infos=%d',
            self.username, self.site_infos_created,
        )
        return user

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.username,
            'site_infos_created': self.site_infos_created,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateUsers(Operation):
    """Migrate all users from mongoengine to beanie."""

    op_name = 'migrate_users'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = list(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped = 0
        self.total_site_infos = 0
        self.total_filtered = 0

    async def execute(self, session: AsyncClientSession) -> list[User]:
        total = GlobalUser.objects.count()
        self.logger.info(
            'MigrateUsers starting: %d v1 users to consider', total,
        )
        sites_by_name = {
            s.name: s for s in await Site.find_all().to_list()
        }
        users = []
        for old_user in GlobalUser.objects.order_by('username'):
            existing = await User.find_one(User.name == old_user.username)
            if existing is not None:
                self.logger.info(
                    'User %s already exists, skipping', old_user.username,
                )
                self.skipped += 1
                continue

            op = MigrateUser(
                self.client, self.author,
                username=old_user.username,
                sites_by_name=sites_by_name,
                sitenames=self.sitenames,
            )
            user = await op.execute(session)
            self.total_site_infos += op.site_infos_created
            self.total_filtered += op.filtered
            users.append(user)
            self.migrated += 1

        self.logger.info(
            'MigrateUsers done: migrated=%d skipped=%d site_infos=%d',
            self.migrated, self.skipped, self.total_site_infos,
        )
        return users

    def describe(self) -> dict[str, Any]:
        return {
            'users_migrated': self.migrated,
            'users_skipped': self.skipped,
            'site_infos_created': self.total_site_infos,
            'site_infos_filtered': self.total_filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateGroups(Operation):
    op_name = 'migrate_groups'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.upgraded = 0
        self.skipped = 0
        self.filtered = 0

    async def _load_users(self, names: set[str]) -> dict[str, User]:
        if not names:
            return {}
        found = await User.find(In(User.name, list(names))).to_list()
        users_by_name = {u.name: u for u in found}
        for missing in names - users_by_name.keys():
            self.logger.warning(
                'Group member %s not found in new database, skipping',
                missing,
            )
        return users_by_name

    @staticmethod
    def _unique_usernames(site_user_refs) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for su in site_user_refs:
            if su.username not in seen:
                seen.add(su.username)
                out.append(su.username)
        return out

    async def execute(self, session: AsyncClientSession) -> list[Group]:
        # v1 access/status groups are owned by MigrateAccessStatusGroups —
        # they become polymorphic AccessGroup/StatusGroup subclasses, not
        # plain Group records, so we filter them out here.
        qs = GlobalGroup.objects(
            type__nin=['access', 'status'],
        ).order_by('groupname')
        total = qs.count()
        self.logger.info(
            'MigrateGroups starting: %d v1 groups to consider '
            '(excluding type in access/status)', total,
        )
        groups = []
        coll = Group.get_pymongo_collection()
        sites_by_name = {
            s.name: s for s in await Site.find_all().to_list()
        }

        for old_group in qs:
            # Probe the raw collection (any _class_id) so we catch legacy
            # docs predating is_root=True. Beanie's polymorphic find_one
            # filters by _class_id and would miss them, leading to a
            # duplicate-key insert.
            raw = await coll.find_one(
                {'name': old_group.groupname}, session=session,
            )
            if raw is not None:
                cid = raw.get('_class_id')
                if cid is None:
                    # Legacy doc. Stamp the discriminator so polymorphic
                    # queries see it going forward. Existing fields stay
                    # — they came from this same migration on a previous
                    # run, so re-collecting members would only churn the
                    # data without changing it.
                    await coll.update_one(
                        {'_id': raw['_id']},
                        {'$set': {'_class_id': Group._class_id}},
                        session=session,
                    )
                    self.upgraded += 1
                    self.logger.info(
                        'Group %s upgraded in place: legacy doc gid=%s '
                        '(stamped _class_id=%r)',
                        old_group.groupname, raw.get('gid'), Group._class_id,
                    )
                    continue
                if cid == Group._class_id:
                    self.logger.info(
                        'Group %s already exists, skipping',
                        old_group.groupname,
                    )
                    self.skipped += 1
                    continue
                # AccessGroup / StatusGroup — name owned by the seed step.
                # Don't overwrite; flag the mismatch for operator review.
                self.logger.warning(
                    'Group %s exists in v2 as %s but v1 type=%r — skipping '
                    '(classification mismatch; investigate)',
                    old_group.groupname, cid, old_group.type,
                )
                self.skipped += 1
                continue

            # Create the (global) Group record first; membership is per-site
            # and lands on GroupMembership edges below.
            group = Group(
                name=old_group.groupname,
                gid=old_group.gid,
                type=old_group.type,
            )
            await group.insert(session=session)
            groups.append(group)
            self.migrated += 1

            # One GroupMembership edge per (user, site) for this group,
            # with `roles` reflecting which v1 buckets the user appeared in.
            # Resolve all usernames across every SiteGroup bucket in one query.
            # Excluded-site buckets are filtered up front so their members
            # are never loaded and produce no edges.
            site_groups = list(SiteGroup.objects(parent=old_group))
            if self.sitenames is not None:
                before_filter = len(site_groups)
                site_groups = [
                    sg for sg in site_groups
                    if sg.sitename in self.sitenames
                ]
                self.filtered += before_filter - len(site_groups)
            all_names: set[str] = set()
            for sg in site_groups:
                for bucket in (sg._members, sg._sponsors, sg._sudoers, sg._slurmers):
                    all_names |= set(self._unique_usernames(bucket))
            users_by_name = await self._load_users(all_names)

            edges_created = 0
            for sg in site_groups:
                site = sites_by_name.get(sg.sitename)
                if site is None:
                    self.logger.warning(
                        'SiteGroup %s references non-existent site %s, '
                        'skipping its memberships',
                        old_group.groupname, sg.sitename,
                    )
                    continue
                roles_by_username: dict[str, set[str]] = {}
                for role, bucket in (
                    ('member', sg._members), ('sponsor', sg._sponsors),
                    ('sudoer', sg._sudoers), ('slurmer', sg._slurmers),
                ):
                    for username in self._unique_usernames(bucket):
                        if username not in users_by_name:
                            continue
                        roles_by_username.setdefault(username, set()).add(role)
                for username, roles in roles_by_username.items():
                    edge = GroupMembership(
                        user=users_by_name[username],
                        group=group,
                        site=site,
                        roles=sorted(roles),
                    )
                    await edge.insert(session=session)
                    edges_created += 1

            self.logger.info(
                'Migrated group %s (gid=%d, %d membership edge(s) across %d site(s))',
                old_group.groupname, old_group.gid,
                edges_created, len(site_groups),
            )

        self.logger.info(
            'MigrateGroups done: migrated=%d upgraded=%d skipped=%d',
            self.migrated, self.upgraded, self.skipped,
        )
        return groups

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'upgraded': self.upgraded,
            'skipped': self.skipped,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


def _old_tres_is_empty(old_tres) -> bool:
    return (
        old_tres.cpus == -1
        and old_tres.gpus == -1
        and old_tres.mem is None
    )


def _new_tres_from_old(old_tres) -> SlurmTRES:
    # The model's validator coerces -1 -> None, but be explicit so the
    # call site documents the v1 sentinel mapping.
    return SlurmTRES(
        cpus=None if old_tres.cpus == -1 else old_tres.cpus,
        gpus=None if old_tres.gpus == -1 else old_tres.gpus,
        mem=old_tres.mem,
    )


class MigrateSlurmPartitions(Operation):
    op_name = 'migrate_slurm_partitions'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped = 0
        self.filtered = 0

    async def execute(self, session: AsyncClientSession) -> list[SlurmPartition]:
        total = SiteSlurmPartition.objects.count()
        self.logger.info(
            'MigrateSlurmPartitions starting: %d v1 partitions to consider',
            total,
        )
        partitions = []
        for old_part in SiteSlurmPartition.objects.order_by('sitename', 'partitionname'):
            if (
                self.sitenames is not None
                and old_part.sitename not in self.sitenames
            ):
                self.filtered += 1
                continue
            site = await Site.find_one(Site.name == old_part.sitename)
            if site is None:
                self.logger.warning(
                    'SiteSlurmPartition %s references non-existent site %s, skipping',
                    old_part.partitionname, old_part.sitename,
                )
                self.skipped += 1
                continue

            existing = await SlurmPartition.find_one(
                SlurmPartition.name == old_part.partitionname,
                SlurmPartition.site.id == site.id,
            )
            if existing is not None:
                self.logger.info(
                    'SlurmPartition %s on %s already exists, skipping',
                    old_part.partitionname, old_part.sitename,
                )
                self.skipped += 1
                continue

            part = SlurmPartition(name=old_part.partitionname, site=site)
            await part.insert(session=session)
            partitions.append(part)
            self.migrated += 1
            self.logger.info(
                'Migrated partition %s on %s', old_part.partitionname, old_part.sitename,
            )

        self.logger.info(
            'MigrateSlurmPartitions done: migrated=%d skipped=%d filtered=%d',
            self.migrated, self.skipped, self.filtered,
        )
        return partitions

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateSlurmQOSes(Operation):
    op_name = 'migrate_slurm_qoses'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped = 0
        self.allocations_created = 0
        self.filtered = 0

    async def _build_alloc_list(
        self,
        session: AsyncClientSession,
        old_tres,
    ) -> list[SlurmAllocation]:
        """Create a single SlurmAllocation from a non-default old TRES; empty list otherwise."""
        if old_tres is None or _old_tres_is_empty(old_tres):
            return []
        alloc = SlurmAllocation(
            tres=_new_tres_from_old(old_tres),
            comment='migrated from v1',
        )
        await alloc.insert(session=session)
        self.allocations_created += 1
        return [alloc]

    async def execute(self, session: AsyncClientSession) -> list[SlurmQOS]:
        total = SiteSlurmQOS.objects.count()
        self.logger.info(
            'MigrateSlurmQOSes starting: %d v1 QOSes to consider', total,
        )
        qoses = []
        for old_qos in SiteSlurmQOS.objects.order_by('sitename', 'qosname'):
            if (
                self.sitenames is not None
                and old_qos.sitename not in self.sitenames
            ):
                self.filtered += 1
                continue
            site = await Site.find_one(Site.name == old_qos.sitename)
            if site is None:
                self.logger.warning(
                    'SiteSlurmQOS %s references non-existent site %s, skipping',
                    old_qos.qosname, old_qos.sitename,
                )
                self.skipped += 1
                continue

            existing = await SlurmQOS.find_one(
                SlurmQOS.name == old_qos.qosname,
                SlurmQOS.site.id == site.id,
            )
            if existing is not None:
                self.logger.info(
                    'SlurmQOS %s on %s already exists, skipping',
                    old_qos.qosname, old_qos.sitename,
                )
                self.skipped += 1
                continue

            group_limits = await self._build_alloc_list(session, old_qos.group_limits)
            user_limits = await self._build_alloc_list(session, old_qos.user_limits)
            job_limits = await self._build_alloc_list(session, old_qos.job_limits)

            qos = SlurmQOS(
                name=old_qos.qosname,
                site=site,
                group_limits=group_limits,
                user_limits=user_limits,
                job_limits=job_limits,
                priority=old_qos.priority or 0,
                flags=list(old_qos.flags) if old_qos.flags else ['DenyOnLimit'],
            )
            await qos.insert(session=session)
            qoses.append(qos)
            self.migrated += 1
            self.logger.info(
                'Migrated QOS %s on %s (group=%d user=%d job=%d allocations)',
                old_qos.qosname, old_qos.sitename,
                len(group_limits), len(user_limits), len(job_limits),
            )

        self.logger.info(
            'MigrateSlurmQOSes done: migrated=%d skipped=%d allocations=%d',
            self.migrated, self.skipped, self.allocations_created,
        )
        return qoses

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
            'allocations_created': self.allocations_created,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


def _old_limits_are_default(old_limits) -> bool:
    if old_limits is None:
        return True
    return (
        old_limits.max_user_jobs == -1
        and old_limits.max_group_jobs == -1
        and old_limits.max_submit_jobs == -1
        and old_limits.max_job_length == '-1'
    )


class MigrateSlurmAccounts(Operation):
    op_name = 'migrate_slurm_accounts'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped = 0
        self.filtered = 0

    async def execute(self, session: AsyncClientSession) -> list[SlurmAccount]:
        total = SiteGroup.objects.count()
        self.logger.info(
            'MigrateSlurmAccounts starting: %d v1 SiteGroup rows to consider',
            total,
        )
        accounts = []
        for site_group in SiteGroup.objects.order_by('sitename', 'groupname'):
            if (
                self.sitenames is not None
                and site_group.sitename not in self.sitenames
            ):
                self.filtered += 1
                continue
            has_assoc = SiteSlurmAssociation.objects(group=site_group).count() > 0
            has_limits = not _old_limits_are_default(site_group.slurm)
            if not (has_assoc or has_limits):
                continue

            site = await Site.find_one(Site.name == site_group.sitename)
            if site is None:
                self.logger.warning(
                    'SiteGroup %s/%s references non-existent site, skipping',
                    site_group.groupname, site_group.sitename,
                )
                self.skipped += 1
                continue

            group = await Group.find_one(Group.name == site_group.groupname)
            if group is None:
                self.logger.warning(
                    'SiteGroup %s/%s has no matching Group in new db, skipping',
                    site_group.groupname, site_group.sitename,
                )
                self.skipped += 1
                continue

            existing = await SlurmAccount.find_one(
                SlurmAccount.group.id == group.id,
                SlurmAccount.site.id == site.id,
            )
            if existing is not None:
                self.logger.info(
                    'SlurmAccount for %s on %s already exists, skipping',
                    site_group.groupname, site_group.sitename,
                )
                self.skipped += 1
                continue

            old_limits = site_group.slurm
            limits = SlurmAccountLimits(
                max_user_jobs=old_limits.max_user_jobs,
                max_group_jobs=old_limits.max_group_jobs,
                max_submit_jobs=old_limits.max_submit_jobs,
                max_job_length=old_limits.max_job_length,
            )

            account = SlurmAccount(
                group=group, site=site, limits=limits, coordinators=[],
            )
            await account.insert(session=session)
            accounts.append(account)
            self.migrated += 1
            self.logger.info(
                'Migrated SlurmAccount for %s on %s',
                site_group.groupname, site_group.sitename,
            )

        self.logger.info(
            'MigrateSlurmAccounts done: migrated=%d skipped=%d filtered=%d',
            self.migrated, self.skipped, self.filtered,
        )
        return accounts

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateSlurmAssociations(Operation):
    op_name = 'migrate_slurm_associations'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped = 0
        self.filtered = 0

    async def execute(self, session: AsyncClientSession) -> list[SlurmAssociation]:
        total = SiteSlurmAssociation.objects.count()
        self.logger.info(
            'MigrateSlurmAssociations starting: %d v1 associations to consider',
            total,
        )
        assocs = []
        for old_assoc in SiteSlurmAssociation.objects.order_by('sitename'):
            if (
                self.sitenames is not None
                and old_assoc.sitename not in self.sitenames
            ):
                self.filtered += 1
                continue
            site = await Site.find_one(Site.name == old_assoc.sitename)
            if site is None:
                self.logger.warning(
                    'SiteSlurmAssociation references non-existent site %s, skipping',
                    old_assoc.sitename,
                )
                self.skipped += 1
                continue

            old_group = old_assoc.group
            old_partition = old_assoc.partition
            old_qos = old_assoc.qos

            group = await Group.find_one(Group.name == old_group.groupname)
            if group is None:
                self.logger.warning(
                    'Association references missing Group %s, skipping',
                    old_group.groupname,
                )
                self.skipped += 1
                continue

            account = await SlurmAccount.find_one(
                SlurmAccount.group.id == group.id,
                SlurmAccount.site.id == site.id,
            )
            if account is None:
                self.logger.warning(
                    'No SlurmAccount for %s on %s (did accounts migrate first?), skipping',
                    old_group.groupname, old_assoc.sitename,
                )
                self.skipped += 1
                continue

            partition = await SlurmPartition.find_one(
                SlurmPartition.name == old_partition.partitionname,
                SlurmPartition.site.id == site.id,
            )
            if partition is None:
                self.logger.warning(
                    'No SlurmPartition %s on %s, skipping association',
                    old_partition.partitionname, old_assoc.sitename,
                )
                self.skipped += 1
                continue

            qos = await SlurmQOS.find_one(
                SlurmQOS.name == old_qos.qosname,
                SlurmQOS.site.id == site.id,
            )
            if qos is None:
                self.logger.warning(
                    'No SlurmQOS %s on %s, skipping association',
                    old_qos.qosname, old_assoc.sitename,
                )
                self.skipped += 1
                continue

            existing = await SlurmAssociation.find_one(
                SlurmAssociation.site.id == site.id,
                SlurmAssociation.account.id == account.id,
                SlurmAssociation.partition.id == partition.id,
                SlurmAssociation.qos.id == qos.id,
            )
            if existing is not None:
                self.logger.info(
                    'SlurmAssociation %s/%s/%s on %s already exists, skipping',
                    old_group.groupname, old_partition.partitionname,
                    old_qos.qosname, old_assoc.sitename,
                )
                self.skipped += 1
                continue

            assoc = SlurmAssociation(
                site=site, account=account, partition=partition, qos=qos,
            )
            await assoc.insert(session=session)
            assocs.append(assoc)
            self.migrated += 1
            self.logger.info(
                'Migrated association %s/%s/%s on %s',
                old_group.groupname, old_partition.partitionname,
                old_qos.qosname, old_assoc.sitename,
            )

        self.logger.info(
            'MigrateSlurmAssociations done: migrated=%d skipped=%d filtered=%d',
            self.migrated, self.skipped, self.filtered,
        )
        return assocs

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


# ---------------------------------------------------------------------------
# Storage migration
# ---------------------------------------------------------------------------
#
# v1 split storage across SourceCollections (per-site defaults containers),
# NFS/ZFS MountSources (with fallback-to-collection host/path/quota), and
# Automounts. v2 separates the provisionable backing entity (StorageVolume,
# identified physically by (site, host, host_path)) from the user-facing
# Storage record (volume + subpath + mount mechanism). Plain NFS exports of
# subdirectories (Farm legacy homes) become Storage.subpath on the covering
# ZFS volume rather than volumes of their own.

# Index of volumes per (site_id, host): list of (PurePosixPath, volume).
VolIndex = dict[tuple[PydanticObjectId, str], list[tuple[PurePosixPath, StorageVolume]]]


def _match_covering_volume(
    vol_index: VolIndex,
    site_id: PydanticObjectId,
    host: str,
    path: PurePosixPath,
    *,
    allow_equal: bool,
) -> tuple[StorageVolume, str] | None:
    """Longest-prefix covering volume on (site, host) for `path`.

    A volume V covers P iff P == V.path (only when `allow_equal`) or V.path
    is a proper ancestor of P. Among covers, the deepest (most specific)
    wins. Returns (volume, subpath) where subpath == '' when P == V.path.
    """
    candidates = []
    for vpath, vol in vol_index.get((site_id, host), ()):
        if path == vpath:
            if allow_equal:
                candidates.append((vpath, vol))
        elif vpath in path.parents:
            candidates.append((vpath, vol))
    if not candidates:
        return None
    vpath, vol = max(candidates, key=lambda c: len(c[0].parts))
    sub = '' if vpath == path else str(path.relative_to(vpath))
    return vol, sub


def _resolve_v1_source(src) -> tuple[str, str]:
    """Concrete (host, host_path) for a v1 mount source via its
    collection-fallback properties. Raises ValueError when unresolvable
    (the v1 properties raise ValueError on missing values, AttributeError
    when collection is None, and mongoengine DoesNotExist when the
    collection DBRef dangles)."""
    try:
        return src.host, str(src.host_path)
    except (ValueError, AttributeError, DoesNotExist) as e:
        raise ValueError(
            f'v1 source {src.sitename}/{src.name} unresolvable: {e}'
        )


def _v1_source_quota(src) -> str | None:
    try:
        return src.quota
    except (ValueError, AttributeError, DoesNotExist):
        return None


def _v1_source_export(src) -> NFSExportConfig | None:
    try:
        opts = src.export_options
    except (ValueError, AttributeError, DoesNotExist):
        opts = ''
    try:
        ranges = list(src.export_ranges)
    except (ValueError, AttributeError, DoesNotExist):
        ranges = []
    if opts or ranges:
        return NFSExportConfig(export_options=opts, export_ranges=ranges)
    return None


class MigrateAutomountMaps(Operation):
    """Migrate v1 AutomountMaps to v2 (tablename → name)."""

    op_name = 'migrate_automount_maps'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped = 0
        self.sites_missing = 0
        self.collisions = 0
        self.filtered = 0

    async def execute(self, session: AsyncClientSession) -> int:
        sites_by_name = {
            s.name: s for s in await Site.find_all().to_list()
        }

        # Multiple v1 maps may share (sitename, tablename) with different
        # prefixes; v2 maps are unique on (name, site), so suffix the name.
        from collections import Counter
        tablename_counts = Counter(
            (m.sitename, m.tablename) for m in OldAutomountMap.objects()
        )

        for old_map in OldAutomountMap.objects().order_by('sitename', 'tablename'):
            if (
                self.sitenames is not None
                and old_map.sitename not in self.sitenames
            ):
                self.filtered += 1
                continue
            site = sites_by_name.get(old_map.sitename)
            if site is None:
                self.logger.warning(
                    'AutomountMap %s/%s references missing site, skipping',
                    old_map.sitename, old_map.tablename,
                )
                self.sites_missing += 1
                continue

            name = old_map.tablename
            if tablename_counts[(old_map.sitename, old_map.tablename)] > 1:
                name = f"{name}-{old_map.prefix.strip('/').replace('/', '-')}"
                self.collisions += 1
                self.logger.warning(
                    'Multiple v1 maps for tablename %s on %s; migrating '
                    'prefix=%s as %r — requires operator review',
                    old_map.tablename, old_map.sitename, old_map.prefix, name,
                )

            existing = await AutomountMap.find_one(
                AutomountMap.name == name,
                AutomountMap.site.id == site.id,
            )
            if existing is not None:
                self.skipped += 1
                continue

            if old_map._add_options or old_map._remove_options:
                self.logger.warning(
                    'AutomountMap %s/%s has _add/_remove options — dead v1 '
                    'fields (never consulted); not migrated',
                    old_map.sitename, old_map.tablename,
                )

            await AutomountMap(
                name=name,
                site=site,
                prefix=old_map.prefix,
                options=list(old_map._options or []),
            ).insert(session=session)
            self.migrated += 1
            self.logger.info(
                'Migrated automount map %s on %s (prefix=%s)',
                name, old_map.sitename, old_map.prefix,
            )

        self.logger.info(
            'MigrateAutomountMaps done: migrated=%d skipped=%d',
            self.migrated, self.skipped,
        )
        return self.migrated

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
            'sites_missing': self.sites_missing,
            'collisions': self.collisions,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateStorageVolumes(Operation):
    """Create v2 StorageVolumes from v1 collections + mount sources.

    Three passes (shallow paths first so nesting parents correctly):
      A. SourceCollections with host+prefix → root volumes; the 'home'
         collection also seeds Site.storage defaults.
      B. ZFSMountSources → quota'd volumes (collection fallbacks resolved
         into concrete values; parent = deepest strictly-covering volume).
      C. Plain NFSMountSources → matched against existing volumes (no doc
         written — the Storage step re-derives subpaths) or, when no
         covering volume exists, a bare unquota'd volume.

    Volume identity (idempotency key) is (site, host, host_path).
    """

    op_name = 'migrate_storage_volumes'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.collection_roots = 0
        self.collections_defaults_only = 0
        self.zfs_volumes = 0
        self.nfs_matched = 0
        self.nfs_bare_volumes = 0
        self.enriched = 0
        self.skipped_existing = 0
        self.unresolvable = 0
        self.parents_linked = 0
        self.name_conflicts = 0
        self.home_settings_set = 0
        self.filtered = 0

    def _filtered_out(self, sitename: str) -> bool:
        if self.sitenames is not None and sitename not in self.sitenames:
            self.filtered += 1
            return True
        return False

    @staticmethod
    async def _build_vol_index() -> tuple[VolIndex, dict[PydanticObjectId, set[str]]]:
        vol_index: VolIndex = {}
        names_by_site: dict[PydanticObjectId, set[str]] = {}
        for vol in await StorageVolume.find_all().to_list():
            site_id = link_target_id(vol.site)
            vol_index.setdefault((site_id, vol.host), []).append(
                (PurePosixPath(vol.host_path), vol)
            )
            names_by_site.setdefault(site_id, set()).add(vol.name)
        return vol_index, names_by_site

    def _pick_name(
        self,
        base: str,
        parent: StorageVolume | None,
        host_path: str,
        site_names: set[str],
    ) -> str:
        if base.startswith('/'):
            base = PurePosixPath(base).name
        name = f'{parent.name}/{base}' if parent is not None else base
        if name in site_names:
            name = host_path.lstrip('/').replace('/', '-')
            self.name_conflicts += 1
            self.logger.warning(
                'Volume name collision for %r; using path slug %r',
                base, name,
            )
        site_names.add(name)
        return name

    async def _insert_volume(
        self,
        session: AsyncClientSession,
        vol_index: VolIndex,
        *,
        name: str,
        site: Site,
        host: str,
        host_path: str,
        parent: StorageVolume | None = None,
        allocations: list[StorageAllocation] | None = None,
        nfs_export: NFSExportConfig | None = None,
        managed: bool = True,
    ) -> StorageVolume:
        # `managed=False` (v1 plain-NFS bare volumes) leaves zfs=None: the
        # volume is an export root we don't provision as a ZFS dataset.
        # ZFSConfig presence is the marker the puppet export classifies on.
        volume = StorageVolume(
            name=name,
            site=site,
            backend='zfs',
            zfs=ZFSConfig() if managed else None,
            host=host,
            host_path=host_path,
            parent=parent,
            allocations=allocations or [],
            nfs_export=nfs_export,
        )
        await volume.insert(session=session)
        vol_index.setdefault((site.id, host), []).append(
            (PurePosixPath(host_path), volume)
        )
        return volume

    async def execute(self, session: AsyncClientSession) -> int:
        sites_by_name = {
            s.name: s for s in await Site.find_all().to_list()
        }
        vol_index, names_by_site = await self._build_vol_index()

        # --- Pass A: collection roots + Site storage defaults ---
        for col in OldNFSSourceCollection.objects():
            if self._filtered_out(col.sitename):
                continue
            site = sites_by_name.get(col.sitename)
            if site is None:
                self.logger.warning(
                    'Collection %s/%s references missing site, skipping',
                    col.sitename, col.name,
                )
                continue
            if not (col._host and col.prefix):
                self.logger.info(
                    'Collection %s/%s is defaults-only (no host/prefix); '
                    'no root volume', col.sitename, col.name,
                )
                self.collections_defaults_only += 1
                continue

            key_path = PurePosixPath(col.prefix)
            existing = _match_covering_volume(
                vol_index, site.id, col._host, key_path, allow_equal=True,
            )
            if existing is not None and existing[1] == '':
                volume = existing[0]
                self.skipped_existing += 1
            else:
                site_names = names_by_site.setdefault(site.id, set())
                name = self._pick_name(col.name, None, col.prefix, site_names)
                volume = await self._insert_volume(
                    session, vol_index,
                    name=name, site=site, host=col._host,
                    host_path=col.prefix,
                )
                self.collection_roots += 1

            if col.name == 'home':
                if site.storage.default_home_volume is None:
                    site.storage.default_home_volume = volume.id
                    site.storage.default_home_quota = getattr(col, '_quota', None)
                    await site.save(session=session)
                    self.home_settings_set += 1
                    self.logger.info(
                        'Set %s default home volume=%s quota=%s',
                        site.name, volume.name,
                        site.storage.default_home_quota,
                    )

        # --- Pass B: ZFS sources → quota'd volumes ---
        zfs_sources = list(OldZFSMountSource.objects())

        def _depth(src) -> int:
            try:
                return len(PurePosixPath(str(src.host_path)).parts)
            except (ValueError, AttributeError, DoesNotExist):
                return 0

        for src in sorted(zfs_sources, key=_depth):
            if self._filtered_out(src.sitename):
                continue
            site = sites_by_name.get(src.sitename)
            if site is None:
                self.logger.warning(
                    'ZFS source %s/%s references missing site, skipping',
                    src.sitename, src.name,
                )
                continue
            try:
                host, host_path = _resolve_v1_source(src)
            except ValueError as e:
                self.logger.warning('%s', e)
                self.unresolvable += 1
                continue

            path = PurePosixPath(host_path)
            hit = _match_covering_volume(
                vol_index, site.id, host, path, allow_equal=True,
            )
            if hit is not None and hit[1] == '':
                # Exact-path volume already exists — either a Pass A
                # collection root (created bare: no quota, no export) or an
                # idempotent re-run. The ZFS source describes the same real
                # dataset, so merge its attributes instead of dropping them
                # (Farm's 'home' share: collection root /nas-4-1/home/ +
                # equal-path ZFS source carrying the 25T quota + exports).
                volume = hit[0]
                updated = False
                quota = _v1_source_quota(src)
                if not volume.allocations and quota:
                    volume.allocations = [StorageAllocation(
                        quota=quota, comment='migrated from v1',
                    )]
                    updated = True
                export = _v1_source_export(src)
                if volume.nfs_export is None and export is not None:
                    volume.nfs_export = export
                    updated = True
                if volume.zfs is None:
                    # a ZFS source proves this is a managed dataset
                    volume.zfs = ZFSConfig()
                    updated = True
                if updated:
                    await volume.save(session=session)
                    self.enriched += 1
                else:
                    self.skipped_existing += 1
                continue

            parent_hit = _match_covering_volume(
                vol_index, site.id, host, path, allow_equal=False,
            )
            parent = parent_hit[0] if parent_hit is not None else None

            quota = _v1_source_quota(src)
            site_names = names_by_site.setdefault(site.id, set())
            name = self._pick_name(src.name, parent, host_path, site_names)
            await self._insert_volume(
                session, vol_index,
                name=name, site=site, host=host, host_path=host_path,
                parent=parent,
                allocations=(
                    [StorageAllocation(quota=quota, comment='migrated from v1')]
                    if quota else []
                ),
                nfs_export=_v1_source_export(src),
            )
            self.zfs_volumes += 1
            if parent is not None:
                self.parents_linked += 1

        # --- Pass C: plain NFS sources → match-or-create ---
        nfs_sources = list(OldNFSMountSource.objects(
            _cls='StorageMountSource.NFSMountSource',
        ))
        for src in sorted(nfs_sources, key=_depth):
            if self._filtered_out(src.sitename):
                continue
            site = sites_by_name.get(src.sitename)
            if site is None:
                self.logger.warning(
                    'NFS source %s/%s references missing site, skipping',
                    src.sitename, src.name,
                )
                continue
            try:
                host, host_path = _resolve_v1_source(src)
            except ValueError as e:
                self.logger.warning('%s', e)
                self.unresolvable += 1
                continue

            path = PurePosixPath(host_path)
            hit = _match_covering_volume(
                vol_index, site.id, host, path, allow_equal=True,
            )
            if hit is not None:
                # Covered by an existing volume — the Storage step derives
                # (volume, subpath); subdir export config lands on Storage.
                self.nfs_matched += 1
                continue

            parent_hit = _match_covering_volume(
                vol_index, site.id, host, path, allow_equal=False,
            )
            parent = parent_hit[0] if parent_hit is not None else None
            site_names = names_by_site.setdefault(site.id, set())
            name = self._pick_name(src.name, parent, host_path, site_names)
            await self._insert_volume(
                session, vol_index,
                name=name, site=site, host=host, host_path=host_path,
                parent=parent,
                nfs_export=_v1_source_export(src),
                managed=False,
            )
            self.nfs_bare_volumes += 1

        self.logger.info(
            'MigrateStorageVolumes done: roots=%d zfs=%d nfs_matched=%d '
            'nfs_bare=%d enriched=%d skipped=%d unresolvable=%d',
            self.collection_roots, self.zfs_volumes, self.nfs_matched,
            self.nfs_bare_volumes, self.enriched, self.skipped_existing,
            self.unresolvable,
        )
        return self.collection_roots + self.zfs_volumes + self.nfs_bare_volumes

    def describe(self) -> dict[str, Any]:
        return {
            'collection_roots': self.collection_roots,
            'collections_defaults_only': self.collections_defaults_only,
            'zfs_volumes': self.zfs_volumes,
            'nfs_matched': self.nfs_matched,
            'nfs_bare_volumes': self.nfs_bare_volumes,
            'enriched': self.enriched,
            'skipped_existing': self.skipped_existing,
            'unresolvable': self.unresolvable,
            'parents_linked': self.parents_linked,
            'name_conflicts': self.name_conflicts,
            'home_settings_set': self.home_settings_set,
            'filtered': self.filtered,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }


class MigrateStorages(Operation):
    """Migrate v1 Storage records (source + Automount) to v2 Storages
    referencing the volumes created by MigrateStorageVolumes.

    The v2 Storage lands on the MOUNT's site (v1's `mount_source_site`
    pattern binds a site-B Automount to a site-A source; v1
    `Storage.sitename` reports the source site, which is wrong for the
    user-facing record). The volume stays on the source's site.
    """

    op_name = 'migrate_storages'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitenames: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.sitenames = set(sitenames) if sitenames else None
        self.migrated = 0
        self.skipped_existing = 0
        self.owners_missing = 0
        self.groups_missing = 0
        self.maps_missing = 0
        self.volumes_missing = 0
        self.unresolvable = 0
        self.cross_site = 0
        self.mounts_unsupported = 0
        self.filtered = 0
        self.dangling_refs = 0

    async def execute(self, session: AsyncClientSession) -> int:
        sites_by_name = {
            s.name: s for s in await Site.find_all().to_list()
        }
        maps_by_key: dict[tuple[PydanticObjectId, str], AutomountMap] = {}
        for amap in await AutomountMap.find_all().to_list():
            maps_by_key[(link_target_id(amap.site), amap.name)] = amap
        vol_index, _ = await MigrateStorageVolumes._build_vol_index()

        # Defensive pre-pass: force every lazy DBRef deref (source, mount,
        # owner, group, and the automount's map) per storage and quarantine
        # records with dangling references — real v1 data contains DBRefs
        # whose targets were deleted, and mongoengine raises DoesNotExist
        # at attribute-access time, which would otherwise abort the whole
        # migration mid-batch.
        old_storages = []
        owner_names: set[str] = set()
        group_names: set[str] = set()
        for s in OldStorage.objects():
            try:
                source, mount = s.source, s.mount
                owner_names.add(source.owner.username)
                group_names.add(source.group.groupname)
                if isinstance(mount, OldAutomount):
                    mount.map.sitename  # noqa: B018 — force map deref
            except DoesNotExist as e:
                self.logger.warning(
                    'Storage %s has a dangling v1 reference (%s), skipping',
                    s.name, e,
                )
                self.dangling_refs += 1
                continue
            old_storages.append(s)

        users_by_name = {
            u.name: u
            for u in await User.find(In(User.name, list(owner_names))).to_list()
        }
        groups_by_name = {
            g.name: g
            for g in await Group.find(In(Group.name, list(group_names))).to_list()
        }

        for old in sorted(old_storages, key=lambda s: s.name):
            mount = old.mount
            if not isinstance(mount, OldAutomount):
                self.logger.warning(
                    'Storage %s has unsupported mount type %s, skipping',
                    old.name, type(mount).__name__,
                )
                self.mounts_unsupported += 1
                continue

            # The filter keys on the MOUNT's site — the site the user-facing
            # record lands on. A storage on an allowed site whose source
            # lives on an excluded site will fail volume resolution below
            # (the excluded site's volumes were never migrated) and be
            # tallied under volumes_missing.
            sitename = mount.map.sitename
            if self.sitenames is not None and sitename not in self.sitenames:
                self.filtered += 1
                continue
            site = sites_by_name.get(sitename)
            if site is None:
                self.logger.warning(
                    'Storage %s references missing site %s, skipping',
                    old.name, sitename,
                )
                continue
            category = mount.map.tablename
            if category not in STORAGE_CATEGORIES:
                self.logger.warning(
                    'Storage %s has non-category tablename %r, skipping',
                    old.name, category,
                )
                continue

            existing = await Storage.find_one(
                Storage.name == old.name,
                Storage.site.id == site.id,
                Storage.category == category,
            )
            if existing is not None:
                self.skipped_existing += 1
                continue

            src = old.source
            src_site = sites_by_name.get(src.sitename)
            if src_site is None:
                self.logger.warning(
                    'Storage %s source references missing site %s, skipping',
                    old.name, src.sitename,
                )
                continue
            if src.sitename != sitename:
                self.cross_site += 1
                self.logger.info(
                    'Storage %s: mount on %s, source on %s (cross-site)',
                    old.name, sitename, src.sitename,
                )

            try:
                host, host_path = _resolve_v1_source(src)
            except ValueError as e:
                self.logger.warning('%s', e)
                self.unresolvable += 1
                continue

            hit = _match_covering_volume(
                vol_index, src_site.id, host, PurePosixPath(host_path),
                allow_equal=True,
            )
            if hit is None:
                self.logger.warning(
                    'Storage %s: no covering volume for %s:%s (run '
                    '`ng migrate storage volumes` first?), skipping',
                    old.name, host, host_path,
                )
                self.volumes_missing += 1
                continue
            volume, subpath = hit

            owner = users_by_name.get(src.owner.username)
            if owner is None:
                self.logger.warning(
                    'Storage %s owner %s not in v2, skipping',
                    old.name, src.owner.username,
                )
                self.owners_missing += 1
                continue
            group = groups_by_name.get(src.group.groupname)
            if group is None:
                self.logger.warning(
                    'Storage %s group %s not in v2, skipping',
                    old.name, src.group.groupname,
                )
                self.groups_missing += 1
                continue

            amap = maps_by_key.get((site.id, category))
            if amap is None:
                self.logger.warning(
                    'Storage %s: no v2 automount map %r on %s, skipping',
                    old.name, category, sitename,
                )
                self.maps_missing += 1
                continue

            # Subdir exports keep their own source-level export config on
            # the Storage record (the volume carries the dataset-level one).
            nfs_export = None
            if subpath and not isinstance(src, OldZFSMountSource):
                if src._export_options or src._export_ranges:
                    nfs_export = NFSExportConfig(
                        export_options=src._export_options or '',
                        export_ranges=list(src._export_ranges or []),
                    )

            await Storage(
                name=old.name,
                site=site,
                category=category,
                owner=owner,
                group=group,
                volume=volume,
                subpath=subpath,
                nfs_export=nfs_export,
                automount_map=amap,
                mount_name=mount.name,
                mount_overrides=MountOverrides(
                    options=list(mount._options or []),
                    add_options=list(mount._add_options or []),
                    remove_options=list(mount._remove_options or []),
                ),
                globus=bool(old.globus),
            ).insert(session=session)
            self.migrated += 1
            self.logger.info(
                'Migrated storage %s on %s (volume=%s subpath=%r)',
                old.name, sitename, volume.name, subpath,
            )

        self.logger.info(
            'MigrateStorages done: migrated=%d skipped=%d volumes_missing=%d '
            'dangling_refs=%d',
            self.migrated, self.skipped_existing, self.volumes_missing,
            self.dangling_refs,
        )
        return self.migrated

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped_existing': self.skipped_existing,
            'owners_missing': self.owners_missing,
            'groups_missing': self.groups_missing,
            'maps_missing': self.maps_missing,
            'volumes_missing': self.volumes_missing,
            'unresolvable': self.unresolvable,
            'cross_site': self.cross_site,
            'mounts_unsupported': self.mounts_unsupported,
            'filtered': self.filtered,
            'dangling_refs': self.dangling_refs,
            'sites_filter': sorted(self.sitenames) if self.sitenames else None,
        }
