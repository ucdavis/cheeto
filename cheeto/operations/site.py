from __future__ import annotations

from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.site import Site
from ..models.user import User
from .base import Operation


class CreateSite(Operation):
    op_name = 'create_site'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        fqdn: str,
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.fqdn = fqdn

    async def execute(self, session: AsyncClientSession) -> Site:
        existing = await Site.find_one(Site.name == self.name)
        if existing is not None:
            raise ValueError(f'Site {self.name} already exists')

        site = Site(name=self.name, fqdn=self.fqdn)
        await site.insert(session=session)
        self._site = site
        return site

    def describe(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'fqdn': self.fqdn,
        }
