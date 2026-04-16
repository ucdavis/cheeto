from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.site import Site
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from .base import Operation


class AddSiteUser(Operation):
    op_name = 'add_site_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        user_name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.user_name = user_name
        self.site_name = site_name

    async def execute(self, session: AsyncClientSession) -> UserSiteInfo:
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        existing = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
            UserSiteInfo.site.id == site.id,
        )
        if existing is not None:
            raise ValueError(
                f'User {self.user_name} already on site {self.site_name}'
            )

        usi = UserSiteInfo(user=user, site=site)
        await usi.insert(session=session)
        self._usi = usi
        return usi

    def describe(self) -> dict[str, Any]:
        return {'user': self.user_name, 'site': self.site_name}


class RemoveSiteUser(Operation):
    op_name = 'remove_site_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        user_name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.user_name = user_name
        self.site_name = site_name

    async def execute(self, session: AsyncClientSession) -> None:
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        usi = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
            UserSiteInfo.site.id == site.id,
        )
        if usi is None:
            raise ValueError(
                f'User {self.user_name} not on site {self.site_name}'
            )

        await usi.delete(session=session)

    def describe(self) -> dict[str, Any]:
        return {'user': self.user_name, 'site': self.site_name}
