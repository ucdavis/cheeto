from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.group import Group, StatusGroup
from ..models.group_site_info import GroupSiteInfo
from ..models.site import Site
from ..models.storage import Storage
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from .base import Operation
from .group_site import ensure_group_site


async def _find_personal_group(user: User) -> Group | None:
    """The user's personal group (same name, type='user'). May be absent
    for rows predating personal-group creation."""
    return await Group.find_one(Group.name == user.name, Group.type == 'user')


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

        # Default new site users to 'active' status if a StatusGroup record
        # exists for it (matches v1 implicit default). Falls back to None
        # gracefully if the seed step hasn't run yet.
        active = await StatusGroup.find_one(StatusGroup.status_name == 'active')
        usi = UserSiteInfo(user=user, site=site, status=active)
        await usi.insert(session=session)

        # The personal group follows its owner onto the site so the user's
        # primary-gid group resolves there.
        personal_group = await _find_personal_group(user)
        if personal_group is not None:
            await ensure_group_site(personal_group, site, session)

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
        self._kept_group_site = False

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

        # The personal group leaves with its owner — unless its home
        # storage still exists at the site (rehome/cleanup pending), in
        # which case presence is kept so the export still resolves the gid.
        personal_group = await _find_personal_group(user)
        if personal_group is not None:
            home_storage = await Storage.find_one(
                Storage.group.id == personal_group.id,
                Storage.site.id == site.id,
            )
            if home_storage is not None:
                self._kept_group_site = True
            else:
                gsi = await GroupSiteInfo.find_one(
                    GroupSiteInfo.group.id == personal_group.id,
                    GroupSiteInfo.site.id == site.id,
                )
                if gsi is not None:
                    await gsi.delete(session=session)

    def describe(self) -> dict[str, Any]:
        return {
            'user': self.user_name,
            'site': self.site_name,
            'kept_group_site': self._kept_group_site,
        }
