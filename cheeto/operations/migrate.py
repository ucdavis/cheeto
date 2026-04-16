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
from ..database.user import GlobalUser, SiteUser
from ..models.group import Group
from ..models.site import Site
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

        ssh_keys = []
        if old_user.ssh_key:
            ssh_keys = [SshKey(key=k) for k in old_user.ssh_key]

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
            ssh_keys=ssh_keys,
            access=list(old_user.access) if old_user.access else ['login-ssh'],
            comments=list(old_user.comments) if old_user.comments else [],
            home_directory=old_user.home_directory,
        )
        await user.insert(session=session)
        logger.info('Migrated user %s (uid=%d)', old_user.username, old_user.uid)

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
                usi.expiry = old_site_user.expiry
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

            # Collect members, sponsors, sudoers from all SiteGroup records
            members = []
            sponsors = []
            sudoers = []
            seen_members = set()
            seen_sponsors = set()
            seen_sudoers = set()

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

            group = Group(
                name=old_group.groupname,
                gid=old_group.gid,
                type=old_group.type,
                members=members,
                sponsors=sponsors,
                sudoers=sudoers,
            )
            await group.insert(session=session)
            groups.append(group)
            self.migrated += 1
            logger.info(
                'Migrated group %s (gid=%d, %d members, %d sponsors, %d sudoers)',
                old_group.groupname, old_group.gid,
                len(members), len(sponsors), len(sudoers),
            )

        return groups

    def describe(self) -> dict[str, Any]:
        return {
            'migrated': self.migrated,
            'skipped': self.skipped,
        }
