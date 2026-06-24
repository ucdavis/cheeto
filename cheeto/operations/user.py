from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..constants import (
    DEFAULT_SHELL,
    MIN_CLASS_ID,
    MIN_SHARED_UID,
    MIN_SYSTEM_UID,
    UINT_MAX,
)
from ..encrypt import get_mcf_hasher, hash_yescrypt
from ..models.base import link_target_id
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo
from ..queries.access_status import (
    find_access_group,
    find_status_group,
    resolve_status_name,
)
from ..queries.user import find_redundant_site_statuses, find_users
from .base import Operation


def _hash_password(plaintext: str) -> str:
    return hash_yescrypt(get_mcf_hasher(), plaintext).decode('UTF-8')


async def _resolve_status_link(name: str | None) -> StatusGroup | None:
    if name is None:
        return None
    sg = await find_status_group(name)
    if sg is None:
        raise ValueError(
            f'No StatusGroup with status_name={name!r}; '
            f'run SeedAccessStatusGroups first'
        )
    return sg


async def _resolve_access_links(names: list[str]) -> list[AccessGroup]:
    out: list[AccessGroup] = []
    for n in names:
        ag = await find_access_group(n)
        if ag is None:
            raise ValueError(
                f'No AccessGroup with access_name={n!r}; '
                f'run SeedAccessStatusGroups first'
            )
        out.append(ag)
    return out


async def clear_user_site_statuses(
    user: User, session: AsyncClientSession | None = None,
) -> int:
    """Set every per-site status override for `user` to None (fall back to
    `User.status`). Writes only the USIs that currently carry an override;
    returns the number cleared. Each save fires `mark_user_ldap_dirty`, so
    the user re-syncs to LDAP under the now-effective global status."""
    usis = await UserSiteInfo.find(UserSiteInfo.user.id == user.id).to_list()
    cleared = 0
    for usi in usis:
        if usi.status is not None:
            usi.status = None
            await usi.save(session=session)
            cleared += 1
    return cleared


async def _get_next_id(min_id: int, max_id: int = UINT_MAX) -> int:
    result = await User.find(
        User.uid >= min_id,
        User.uid < max_id,
    ).sort('-uid').limit(1).to_list()
    if result:
        return result[0].uid + 1
    return min_id


class CreateUser(Operation):
    op_name = 'create_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        email: str,
        uid: int,
        fullname: str,
        type: str = 'user',
        shell: str = DEFAULT_SHELL,
        status: str = 'active',
        access: list[str] | None = None,
        gid: int | None = None,
        home_directory: str | None = None,
        password: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.email = email
        self.uid = uid
        self.gid = gid if gid is not None else uid
        self.fullname = fullname
        self.type = type
        self.shell = shell
        self.status = status
        # Empty default — callers explicitly pass access shorthands they need.
        # AccessGroup resolution happens at execute() time so the operation
        # surfaces a clear error if SeedAccessStatusGroups hasn't been run.
        self.access = access or []
        self.home_directory = home_directory or f'/home/{name}'
        self.password = password

    async def execute(self, session: AsyncClientSession) -> tuple[User, Group]:
        existing = await User.find_one(User.name == self.name)
        if existing is not None:
            raise ValueError(f'User {self.name} already exists')

        # Resolve status/access shorthands to StatusGroup/AccessGroup records.
        # Either field may be unset (status=None / access=[]) for system-style
        # accounts that don't participate in the access pipeline.
        status_link = await _resolve_status_link(self.status)
        access_links = await _resolve_access_links(self.access)

        user = User(
            name=self.name,
            email=self.email,
            uid=self.uid,
            gid=self.gid,
            fullname=self.fullname,
            type=self.type,
            shell=self.shell,
            status=status_link,
            access=access_links,
            home_directory=self.home_directory,
            password=_hash_password(self.password) if self.password else None,
        )
        await user.insert(session=session)

        # Primary (user-private) group. Membership is implicit via User.gid;
        # there is no GroupMembership edge for the primary group, and CreateUser
        # has no site context to scope one to anyway.
        group = Group(
            name=self.name,
            gid=self.gid,
            type='user',
        )
        await group.insert(session=session)

        self._user = user
        self._group = group
        return user, group

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'uid': self.uid,
            'gid': self.gid,
            'type': self.type,
            'email': self.email,
            'has_password': self.password is not None,
        }


class CreateSystemUser(Operation):
    op_name = 'create_system_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        email: str,
        fullname: str,
        password: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.email = email
        self.fullname = fullname
        self.password = password

    async def execute(self, session: AsyncClientSession) -> tuple[User, Group]:
        uid = await _get_next_id(MIN_SYSTEM_UID)
        op = CreateUser(
            self.client, self.author,
            name=self.name, email=self.email, uid=uid,
            fullname=self.fullname, type='system',
            shell='/usr/sbin/nologin',
            password=self.password,
        )
        user, group = await op.execute(session)
        self._user = user
        self._group = group
        return user, group

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'type': 'system',
            'email': self.email,
            'has_password': self.password is not None,
        }


class CreateClassUser(Operation):
    op_name = 'create_class_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        email: str,
        fullname: str,
        password: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.email = email
        self.fullname = fullname
        self.password = password

    async def execute(self, session: AsyncClientSession) -> tuple[User, Group]:
        uid = await _get_next_id(MIN_CLASS_ID)
        op = CreateUser(
            self.client, self.author,
            name=self.name, email=self.email, uid=uid,
            fullname=self.fullname, type='class',
            password=self.password,
        )
        user, group = await op.execute(session)
        self._user = user
        self._group = group
        return user, group

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'type': 'class',
            'email': self.email,
            'has_password': self.password is not None,
        }


class CreateSharedUser(Operation):
    op_name = 'create_shared_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        email: str,
        fullname: str,
        password: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.email = email
        self.fullname = fullname
        self.password = password

    async def execute(self, session: AsyncClientSession) -> tuple[User, Group]:
        uid = await _get_next_id(MIN_SHARED_UID)
        op = CreateUser(
            self.client, self.author,
            name=self.name, email=self.email, uid=uid,
            fullname=self.fullname, type='shared',
            password=self.password,
        )
        user, group = await op.execute(session)
        self._user = user
        self._group = group
        return user, group

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'type': 'shared',
            'email': self.email,
            'has_password': self.password is not None,
        }


class SetUserStatus(Operation):
    op_name = 'set_user_status'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        status: str,
        reason: str,
        site: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.status = status
        self.reason = reason
        self.site = site

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')

        status_link = await _resolve_status_link(self.status)

        if self.site is None:
            # Clear the pending offboarding expiry when reactivating, so the
            # next IAM sync doesn't immediately re-offboard the user.
            if (
                self.status == 'active'
                and await resolve_status_name(user.status) == 'offboarding'
            ):
                user.expires_at = None
            user.status = status_link
        else:
            from ..models.site import Site
            from ..models.user_site_info import UserSiteInfo

            site = await Site.find_one(Site.name == self.site)
            if site is None:
                raise ValueError(f'Site {self.site} does not exist')
            usi = await UserSiteInfo.find_one(
                UserSiteInfo.user.id == user.id,
                UserSiteInfo.site.id == site.id,
            )
            if usi is None:
                raise ValueError(f'User {self.name} not on site {self.site}')
            if (
                self.status == 'active'
                and await resolve_status_name(usi.status) == 'offboarding'
            ):
                usi.expires_at = None
            usi.status = status_link
            await usi.save(session=session)

        comment = (
            f'status={self.status}, '
            f'scope={self.site or "global"}, '
            f'reason={self.reason}'
        )
        user.comments.append(comment)
        await user.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'status': self.status,
            'reason': self.reason,
            'site': self.site,
        }


class SetUserType(Operation):
    op_name = 'set_user_type'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        type: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.type = type

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')
        user.type = self.type
        await user.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'username': self.name, 'type': self.type}


class SetUserShell(Operation):
    op_name = 'set_user_shell'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        shell: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.shell = shell

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')
        user.shell = self.shell
        await user.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'username': self.name, 'shell': self.shell}


class SetUserPassword(Operation):
    op_name = 'set_user_password'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        password: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.password = password

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')
        user.password = _hash_password(self.password)
        await user.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'username': self.name}


class AddUserAccess(Operation):
    op_name = 'add_user_access'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        access: str | list[str],
        site: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.access = [access] if isinstance(access, str) else access
        self.site = site

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')

        new_links = await _resolve_access_links(self.access)
        new_ids = {ag.id for ag in new_links}

        if self.site is None:
            existing_ids = {link.ref.id for link in user.access}
            for ag in new_links:
                if ag.id not in existing_ids:
                    user.access.append(ag)
            await user.save(session=session)
        else:
            from ..models.site import Site
            from ..models.user_site_info import UserSiteInfo

            site = await Site.find_one(Site.name == self.site)
            if site is None:
                raise ValueError(f'Site {self.site} does not exist')
            usi = await UserSiteInfo.find_one(
                UserSiteInfo.user.id == user.id,
                UserSiteInfo.site.id == site.id,
            )
            if usi is None:
                raise ValueError(f'User {self.name} not on site {self.site}')
            existing_ids = {link.ref.id for link in usi.access}
            for ag in new_links:
                if ag.id not in existing_ids:
                    usi.access.append(ag)
            await usi.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'access': self.access,
            'site': self.site,
        }


class RemoveUserAccess(Operation):
    op_name = 'remove_user_access'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        access: str | list[str],
        site: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.access = [access] if isinstance(access, str) else access
        self.site = site

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')

        # Resolve to AccessGroup ids so we can filter Link list by ref.id.
        targets = await _resolve_access_links(self.access)
        target_ids = {ag.id for ag in targets}

        if self.site is None:
            user.access = [link for link in user.access if link.ref.id not in target_ids]
            await user.save(session=session)
        else:
            from ..models.site import Site
            from ..models.user_site_info import UserSiteInfo

            site = await Site.find_one(Site.name == self.site)
            if site is None:
                raise ValueError(f'Site {self.site} does not exist')
            usi = await UserSiteInfo.find_one(
                UserSiteInfo.user.id == user.id,
                UserSiteInfo.site.id == site.id,
            )
            if usi is None:
                raise ValueError(f'User {self.name} not on site {self.site}')
            usi.access = [link for link in usi.access if link.ref.id not in target_ids]
            await usi.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'access': self.access,
            'site': self.site,
        }


class AddUserSshKey(Operation):
    """Insert an SshKey for `name`. If `replace=True`, delete every existing
    SshKey for that user first so the new key is the only one."""

    op_name = 'add_user_ssh_key'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        key: str,
        replace: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.key = key.strip()
        self.replace = replace
        self._replaced_count = 0

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')

        if self.replace:
            existing = await SshKey.find(SshKey.user.id == user.id).to_list()
            self._replaced_count = len(existing)
            for k in existing:
                await k.delete(session=session)

        await SshKey(key=self.key, user=user).insert(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.name,
            'replace': self.replace,
            'replaced_count': self._replaced_count,
            'key_fingerprint': (
                self.key[:32] + '...' if len(self.key) > 32 else self.key
            ),
        }


class RemoveUserSshKey(Operation):
    """Delete one SshKey identified by its document id."""

    op_name = 'remove_user_ssh_key'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        key_id: Any,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.key_id = key_id

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')
        key = await SshKey.get(self.key_id)
        if key is None or key.user.ref.id != user.id:
            raise ValueError(
                f'SshKey {self.key_id} not found for user {self.name}'
            )
        await key.delete(session=session)

    def describe(self) -> dict[str, Any]:
        return {'username': self.name, 'key_id': str(self.key_id)}


class AddUserComment(Operation):
    op_name = 'add_user_comment'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        comment: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.comment = comment

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.name)
        if user is None:
            raise ValueError(f'User {self.name} does not exist')
        user.comments.append(self.comment)
        await user.save(session=session)

    def describe(self) -> dict[str, Any]:
        return {'username': self.name, 'comment': self.comment}


class ClearOffboardingSiteStatuses(Operation):
    """One-time backfill: clear every per-site status override for users
    whose global status is `offboarding`, so the effective status falls back
    to the global `offboarding` and they project into `offboarding-users`.

    Non-transactional: it touches hundreds of users / thousands of USIs,
    which would exceed the server transaction limits; each USI save is its
    own atomic write."""

    op_name = 'clear_offboarding_site_statuses'
    transactional = False

    def __init__(self, client: AsyncMongoClient, author: User | None) -> None:
        super().__init__(client, author)
        self._users = 0
        self._cleared = 0

    async def execute(self, session: AsyncClientSession) -> dict[str, int]:
        users = await find_users(status='offboarding')
        self._users = len(users)
        for user in users:
            self._cleared += await clear_user_site_statuses(user, session)
        return {'users': self._users, 'cleared': self._cleared}

    def describe(self) -> dict[str, Any]:
        return {'users': self._users, 'cleared': self._cleared}


class ClearRedundantSiteStatuses(Operation):
    """Maintenance: clear per-site status overrides that merely duplicate the
    user's global status (migration noise). Divergent overrides — a per-site
    status that differs from the global one (e.g. a `disabled` Farm override
    on a globally-`active` user) — are left untouched.

    Non-transactional for the same reason as `ClearOffboardingSiteStatuses`."""

    op_name = 'clear_redundant_site_statuses'
    transactional = False

    def __init__(self, client: AsyncMongoClient, author: User | None) -> None:
        super().__init__(client, author)
        self._cleared = 0

    async def execute(self, session: AsyncClientSession) -> dict[str, int]:
        users = await User.find_all().to_list()
        global_status = {u.id: link_target_id(u.status) for u in users}
        usis = await UserSiteInfo.find_all().to_list()
        for usi in usis:
            if usi.status is None:
                continue
            uid = link_target_id(usi.user)
            if global_status.get(uid) == link_target_id(usi.status):
                usi.status = None
                await usi.save(session=session)
                self._cleared += 1
        return {'cleared': self._cleared}

    def describe(self) -> dict[str, Any]:
        return {'cleared': self._cleared}
