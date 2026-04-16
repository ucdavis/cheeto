from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.history import History
from ..models.user import User


class Operation(ABC):
    """Base class for all database write operations.

    Subclasses must implement:
        execute(session) -> Any   : perform the actual writes
        describe()       -> dict  : summarize the operation for history logging
    """

    op_name: str

    def __init__(self, client: AsyncMongoClient, author: User | None) -> None:
        self.client = client
        self.author = author
        self.logger = logging.getLogger(
            f'{__name__}.{self.__class__.__name__}'
        )

    @abstractmethod
    async def execute(self, session: AsyncClientSession) -> Any:
        ...

    @abstractmethod
    def describe(self) -> dict:
        ...

    async def _run(self) -> Any:
        async with self.client.start_session() as session:
            async with await session.start_transaction():
                result = await self.execute(session)
                await History(
                    op=self.op_name,
                    author=self.author,
                    changes=self.describe(),
                    timestamp=datetime.now(timezone.utc),
                ).insert(session=session)
                self.logger.info(
                    '%s by %s: %s',
                    self.op_name,
                    self.author.name if self.author else None,
                    self.describe(),
                )
        return result

    @classmethod
    async def run(cls, client: AsyncMongoClient, author: User | None, **kwargs: Any) -> Any:
        op = cls(client, author, **kwargs)
        return await op._run()
