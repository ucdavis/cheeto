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
from ..models.group import Group
from ..models.user import User
from .base import Operation


def _hash_password(plaintext: str) -> str:
    return hash_yescrypt(get_mcf_hasher(), plaintext).decode('UTF-8')


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
        self.access = access or ['login-ssh']
        self.home_directory = home_directory or f'/home/{name}'
        self.password = password

    async def execute(self, session: AsyncClientSession) -> tuple[User, Group]:
        existing = await User.find_one(User.name == self.name)
        if existing is not None:
            raise ValueError(f'User {self.name} already exists')

        user = User(
            name=self.name,
            email=self.email,
            uid=self.uid,
            gid=self.gid,
            fullname=self.fullname,
            type=self.type,
            shell=self.shell,
            status=self.status,
            access=self.access,
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

        if self.site is None:
            user.status = self.status
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
            usi.status = self.status
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

        if self.site is None:
            for a in self.access:
                if a not in user.access:
                    user.access.append(a)
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
            for a in self.access:
                if a not in usi.access:
                    usi.access.append(a)
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

        if self.site is None:
            user.access = [a for a in user.access if a not in self.access]
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
            usi.access = [a for a in usi.access if a not in self.access]
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
