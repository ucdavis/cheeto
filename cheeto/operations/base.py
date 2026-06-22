from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Final

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.history import History
from ..models.ldap_sync import deferred_ldap_touches
from ..models.user import User


class _UnsetType:
    """Singleton sentinel used to mark 'caller did not pass a value' on
    operation kwargs. Distinct from None, which means 'set to null'."""

    __slots__ = ()

    def __repr__(self) -> str:
        return 'UNSET'

    def __bool__(self) -> bool:
        return False


UNSET: Final = _UnsetType()


class Operation(ABC):
    """Base class for all database write operations.

    Subclasses must implement:
        execute(session) -> Any   : perform the actual writes
        describe()       -> dict  : summarize the operation for history logging
    """

    op_name: str

    # Whether _run wraps execute() + the History insert in a Mongo
    # transaction. Operations whose side effects are external and
    # non-rollbackable (e.g. shelling out to sacctmgr/ldap) and which may run
    # longer than the server's transactionLifetimeLimit should opt out by
    # setting this False; execute() + History then run in a bare session.
    transactional: bool = True

    def __init__(self, client: AsyncMongoClient, author: User | None) -> None:
        self.client = client
        self.author = author
        # Set per-run via run(skip_history=...). When True, _execute_and_log
        # skips the History insert but still logs — for frequently-hit
        # read-only ops (e.g. the API exports) that would otherwise spam it.
        self.skip_history = False
        self.logger = logging.getLogger(
            f'{__name__}.{self.__class__.__name__}'
        )

    @abstractmethod
    async def execute(self, session: AsyncClientSession) -> Any:
        ...

    @abstractmethod
    def describe(self) -> dict:
        ...

    async def _execute_and_log(self, session: AsyncClientSession) -> Any:
        result = await self.execute(session)
        if not self.skip_history:
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

    async def _run(self) -> Any:
        # LDAP dirty-propagation hooks write outside the session; deferring
        # them until after the transaction commits prevents deadlocks with
        # this operation's own writes (and drops them on rollback). See
        # cheeto/models/ldap_sync.py.
        async with deferred_ldap_touches():
            async with self.client.start_session() as session:
                if self.transactional:
                    async with await session.start_transaction():
                        return await self._execute_and_log(session)
                return await self._execute_and_log(session)

    @classmethod
    async def run(cls, client: AsyncMongoClient, author: User | None,
                  *, skip_history: bool = False, **kwargs: Any) -> Any:
        op = cls(client, author, **kwargs)
        op.skip_history = skip_history
        return await op._run()
