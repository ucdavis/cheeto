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

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..database.group import GlobalGroup, SiteGroup
from ..database.site import Site as OldSite
from ..database.slurm import (
    SiteSlurmAssociation,
    SiteSlurmPartition,
    SiteSlurmQOS,
)
from ..database.user import GlobalUser, SiteUser
from ..models.group import Group
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
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo
from .base import Operation

logger = logging.getLogger(__name__)


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
        sites = []
        for old_site in OldSite.objects():
            existing = await Site.find_one(Site.name == old_site.sitename)
            if existing is not None:
                logger.info('Site %s already exists, skipping', old_site.sitename)
                self.skipped += 1
                continue

            site = Site(
                name=old_site.sitename,
                fqdn=old_site.fqdn,
            )
            await site.insert(session=session)
            sites.append(site)
            self.migrated += 1
            logger.info('Migrated site %s', old_site.sitename)

        return sites

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
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
    ) -> None:
        super().__init__(client, author)
        self.username = username
        self.site_infos_created = 0

    async def execute(self, session: AsyncClientSession) -> User:
        existing = await User.find_one(User.name == self.username)
        if existing is not None:
            raise ValueError(f'User {self.username} already exists in new database')

        old_user = GlobalUser.objects(username=self.username).first()
        if old_user is None:
            raise ValueError(f'User {self.username} not found in old database')

        user = User(
            name=old_user.username,
            email=old_user.email,
            uid=old_user.uid,
            gid=old_user.gid,
            fullname=old_user.fullname,
            shell=old_user.shell,
            type=old_user.type,
            status=old_user.status,
            password=old_user.password,
            access=list(old_user.access) if old_user.access else ['login-ssh'],
            comments=list(old_user.comments) if old_user.comments else [],
            home_directory=old_user.home_directory,
        )
        await user.insert(session=session)
        logger.info('Migrated user %s (uid=%d)', old_user.username, old_user.uid)

        if old_user.ssh_key:
            for k in old_user.ssh_key:
                await SshKey(key=k, user=user).insert(session=session)

        # Migrate SiteUser records for this user
        for old_site_user in SiteUser.objects(username=self.username):
            site = await Site.find_one(Site.name == old_site_user.sitename)
            if site is None:
                logger.warning(
                    'SiteUser %s references non-existent site %s, skipping',
                    self.username, old_site_user.sitename,
                )
                continue

            usi = UserSiteInfo(
                user=user,
                site=site,
                status=old_site_user._status or 'active',
                access=list(old_site_user._access) if old_site_user._access else ['login-ssh'],
            )
            if old_site_user.expiry:
                usi.expires_at = old_site_user.expiry
            await usi.insert(session=session)
            self.site_infos_created += 1

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
        users = []
        for old_user in GlobalUser.objects.order_by('username'):
            existing = await User.find_one(User.name == old_user.username)
            if existing is not None:
                logger.info('User %s already exists, skipping', old_user.username)
                self.skipped += 1
                continue

            op = MigrateUser(
                self.client, self.author,
                username=old_user.username,
            )
            user = await op.execute(session)
            self.total_site_infos += op.site_infos_created
            users.append(user)
            self.migrated += 1

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
        self.skipped = 0

    async def _resolve_members(self, site_user_refs: list) -> list[User]:
        """Resolve old SiteUser references to new User documents."""
        users = []
        seen = set()
        for site_user in site_user_refs:
            username = site_user.username
            if username in seen:
                continue
            seen.add(username)
            user = await User.find_one(User.name == username)
            if user is not None:
                users.append(user)
            else:
                logger.warning(
                    'Group member %s not found in new database, skipping',
                    username,
                )
        return users

    async def execute(self, session: AsyncClientSession) -> list[Group]:
        groups = []

        for old_group in GlobalGroup.objects.order_by('groupname'):
            existing = await Group.find_one(Group.name == old_group.groupname)
            if existing is not None:
                logger.info('Group %s already exists, skipping', old_group.groupname)
                self.skipped += 1
                continue

            # Collect members, sponsors, sudoers, slurmers from all SiteGroup records
            members = []
            sponsors = []
            sudoers = []
            slurmers = []
            seen_members = set()
            seen_sponsors = set()
            seen_sudoers = set()
            seen_slurmers = set()

            for site_group in SiteGroup.objects(parent=old_group):
                for su in site_group._members:
                    if su.username not in seen_members:
                        seen_members.add(su.username)
                        user = await User.find_one(User.name == su.username)
                        if user is not None:
                            members.append(user)

                for su in site_group._sponsors:
                    if su.username not in seen_sponsors:
                        seen_sponsors.add(su.username)
                        user = await User.find_one(User.name == su.username)
                        if user is not None:
                            sponsors.append(user)

                for su in site_group._sudoers:
                    if su.username not in seen_sudoers:
                        seen_sudoers.add(su.username)
                        user = await User.find_one(User.name == su.username)
                        if user is not None:
                            sudoers.append(user)

                for su in site_group._slurmers:
                    if su.username not in seen_slurmers:
                        seen_slurmers.add(su.username)
                        user = await User.find_one(User.name == su.username)
                        if user is not None:
                            slurmers.append(user)

            group = Group(
                name=old_group.groupname,
                gid=old_group.gid,
                type=old_group.type,
                members=members,
                sponsors=sponsors,
                sudoers=sudoers,
                slurmers=slurmers,
            )
            await group.insert(session=session)
            groups.append(group)
            self.migrated += 1
            logger.info(
                'Migrated group %s (gid=%d, %d members, %d sponsors, %d sudoers, %d slurmers)',
                old_group.groupname, old_group.gid,
                len(members), len(sponsors), len(sudoers), len(slurmers),
            )

        return groups

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
        }


def _old_tres_is_empty(old_tres) -> bool:
    return (
        old_tres.cpus == -1
        and old_tres.gpus == -1
        and old_tres.mem is None
    )


def _new_tres_from_old(old_tres) -> SlurmTRES:
    return SlurmTRES(
        cpus=old_tres.cpus,
        gpus=old_tres.gpus,
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
        partitions = []
        for old_part in SiteSlurmPartition.objects.order_by('sitename', 'partitionname'):
            site = await Site.find_one(Site.name == old_part.sitename)
            if site is None:
                logger.warning(
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
                logger.info(
                    'SlurmPartition %s on %s already exists, skipping',
                    old_part.partitionname, old_part.sitename,
                )
                self.skipped += 1
                continue

            part = SlurmPartition(name=old_part.partitionname, site=site)
            await part.insert(session=session)
            partitions.append(part)
            self.migrated += 1
            logger.info(
                'Migrated partition %s on %s', old_part.partitionname, old_part.sitename,
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
        qoses = []
        for old_qos in SiteSlurmQOS.objects.order_by('sitename', 'qosname'):
            site = await Site.find_one(Site.name == old_qos.sitename)
            if site is None:
                logger.warning(
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
                logger.info(
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
            logger.info(
                'Migrated QOS %s on %s (group=%d user=%d job=%d allocations)',
                old_qos.qosname, old_qos.sitename,
                len(group_limits), len(user_limits), len(job_limits),
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
        accounts = []
        for site_group in SiteGroup.objects.order_by('sitename', 'groupname'):
            has_assoc = SiteSlurmAssociation.objects(group=site_group).count() > 0
            has_limits = not _old_limits_are_default(site_group.slurm)
            if not (has_assoc or has_limits):
                continue

            site = await Site.find_one(Site.name == site_group.sitename)
            if site is None:
                logger.warning(
                    'SiteGroup %s/%s references non-existent site, skipping',
                    site_group.groupname, site_group.sitename,
                )
                self.skipped += 1
                continue

            group = await Group.find_one(Group.name == site_group.groupname)
            if group is None:
                logger.warning(
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
                logger.info(
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
            logger.info(
                'Migrated SlurmAccount for %s on %s',
                site_group.groupname, site_group.sitename,
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
        assocs = []
        for old_assoc in SiteSlurmAssociation.objects.order_by('sitename'):
            site = await Site.find_one(Site.name == old_assoc.sitename)
            if site is None:
                logger.warning(
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
                logger.warning(
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
                logger.warning(
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
                logger.warning(
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
                logger.warning(
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
                self.skipped += 1
                continue

            assoc = SlurmAssociation(
                site=site, account=account, partition=partition, qos=qos,
            )
            await assoc.insert(session=session)
            assocs.append(assoc)
            self.migrated += 1
            logger.info(
                'Migrated association %s/%s/%s on %s',
                old_group.groupname, old_partition.partitionname,
                old_qos.qosname, old_assoc.sitename,
            )

        return assocs

    def describe(self) -> dict[str, Any]:
        return {'migrated': self.migrated, 'skipped': self.skipped}
