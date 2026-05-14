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
from ..models.group import AccessGroup, Group, StatusGroup
from ..models.user import User
from ..queries.access_status import find_access_group, find_status_group
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

        group = Group(
            name=self.name,
            gid=self.gid,
            type='user',
            members=[user],
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
