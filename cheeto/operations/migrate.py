"""Migration operations: move data from old mongoengine collections to new beanie models.

These operations read from the old mongoengine documents (GlobalUser, SiteUser,
GlobalGroup, SiteGroup, Site) and create corresponding new beanie documents
(User, UserSiteInfo, Group, Site).

The mongoengine connection must already be established (for reads) and beanie
must be initialized (for writes).
"""

from __future__ import annotations

import logging
from typing import Any

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..database.group import GlobalGroup, SiteGroup
from ..database.site import Site as OldSite
from ..database.slurm import (
    SiteSlurmAssociation,
    SiteSlurmPartition,
    SiteSlurmQOS,
)
from ..constants import MIN_SPECIAL_GID
from ..database.user import GlobalUser, SiteUser
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
    ) -> None:
        super().__init__(client, author)
        self.migrated = 0
        self.skipped = 0

    async def execute(self, session: AsyncClientSession) -> list[Site]:
        total = OldSite.objects.count()
        self.logger.info(
            'MigrateSites starting: %d v1 sites to consider', total,
        )
        sites = []
        for old_site in OldSite.objects():
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
            'MigrateSites done: migrated=%d skipped=%d',
            self.migrated, self.skipped,
        )
        return sites

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
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
    ) -> None:
        super().__init__(client, author)
        self.sites_updated = 0
        self.groups_added = 0
        self.slurmers_added = 0
        self.groups_missing = 0
        self.accounts_missing = 0

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
            (a.group.ref.id, a.site.ref.id): a
            for a in await SlurmAccount.find_all().to_list()
        }

        for old_site in OldSite.objects():
            new_site = await Site.find_one(Site.name == old_site.sitename)
            if new_site is None:
                self.logger.warning(
                    'Site %s not in beanie; skipping globals fold',
                    old_site.sitename,
                )
                continue

            existing_group_ids = {
                link.ref.id for link in new_site.group.sticky
            }
            existing_account_ids = {
                link.ref.id for link in new_site.slurm.sticky
            }
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
                new_site.group.sticky.append(grp)
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
                new_site.slurm.sticky.append(account)
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
    ) -> None:
        super().__init__(client, author)
        self.username = username
        self.site_infos_created = 0
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
        old_site_users = list(SiteUser.objects(username=self.username))
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
        }


class MigrateUsers(Operation):
    """Migrate all users from mongoengine to beanie."""

    op_name = 'migrate_users'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
    ) -> None:
        super().__init__(client, author)
        self.migrated = 0
        self.skipped = 0
        self.total_site_infos = 0

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
            )
            user = await op.execute(session)
            self.total_site_infos += op.site_infos_created
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
        }


class MigrateGroups(Operation):
    op_name = 'migrate_groups'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
    ) -> None:
        super().__init__(client, author)
        self.migrated = 0
        self.upgraded = 0
        self.skipped = 0

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
            site_groups = list(SiteGroup.objects(parent=old_group))
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
    ) -> None:
        super().__init__(client, author)
        self.migrated = 0
        self.skipped = 0

    async def execute(self, session: AsyncClientSession) -> list[SlurmPartition]:
        total = SiteSlurmPartition.objects.count()
        self.logger.info(
            'MigrateSlurmPartitions starting: %d v1 partitions to consider',
            total,
        )
        partitions = []
        for old_part in SiteSlurmPartition.objects.order_by('sitename', 'partitionname'):
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
            'MigrateSlurmPartitions done: migrated=%d skipped=%d',
            self.migrated, self.skipped,
        )
        return partitions

    def describe(self) -> dict[str, Any]:
        return {'migrated': self.migrated, 'skipped': self.skipped}


class MigrateSlurmQOSes(Operation):
    op_name = 'migrate_slurm_qoses'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
    ) -> None:
        super().__init__(client, author)
        self.migrated = 0
        self.skipped = 0
        self.allocations_created = 0

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
    ) -> None:
        super().__init__(client, author)
        self.migrated = 0
        self.skipped = 0

    async def execute(self, session: AsyncClientSession) -> list[SlurmAccount]:
        total = SiteGroup.objects.count()
        self.logger.info(
            'MigrateSlurmAccounts starting: %d v1 SiteGroup rows to consider',
            total,
        )
        accounts = []
        for site_group in SiteGroup.objects.order_by('sitename', 'groupname'):
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
            'MigrateSlurmAccounts done: migrated=%d skipped=%d',
            self.migrated, self.skipped,
        )
        return accounts

    def describe(self) -> dict[str, Any]:
        return {'migrated': self.migrated, 'skipped': self.skipped}


class MigrateSlurmAssociations(Operation):
    op_name = 'migrate_slurm_associations'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
    ) -> None:
        super().__init__(client, author)
        self.migrated = 0
        self.skipped = 0

    async def execute(self, session: AsyncClientSession) -> list[SlurmAssociation]:
        total = SiteSlurmAssociation.objects.count()
        self.logger.info(
            'MigrateSlurmAssociations starting: %d v1 associations to consider',
            total,
        )
        assocs = []
        for old_assoc in SiteSlurmAssociation.objects.order_by('sitename'):
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
            'MigrateSlurmAssociations done: migrated=%d skipped=%d',
            self.migrated, self.skipped,
        )
        return assocs

    def describe(self) -> dict[str, Any]:
        return {'migrated': self.migrated, 'skipped': self.skipped}
