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


# Registry of concrete Operation subclasses keyed by op_name, populated
# automatically via Operation.__init_subclass__. Lets callers discover/look up
# ops by name (e.g. the `cheeto ng history --op` choices). Fully populated once
# `cheeto.operations` is imported — its __init__ imports every op module.
OPERATIONS: dict[str, type['Operation']] = {}


def operation_names() -> list[str]:
    """Sorted op_names of every registered Operation subclass."""
    return sorted(OPERATIONS)


def get_operation(op_name: str) -> type['Operation'] | None:
    """The Operation subclass registered under `op_name`, or None."""
    return OPERATIONS.get(op_name)


class Operation(ABC):
    """Base class for all database write operations.

    Subclasses must implement:
        execute(session) -> Any   : perform the actual writes
        describe()       -> dict  : summarize the operation for history logging
    """

    op_name: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Register each concrete op by its own op_name (not one inherited from
        # a parent op). Fires for subclasses at any depth, so ops that extend
        # other ops register too.
        super().__init_subclass__(**kwargs)
        op_name = cls.__dict__.get('op_name')
        if op_name:
            existing = OPERATIONS.get(op_name)
            if existing is not None and existing is not cls:
                raise ValueError(
                    f'duplicate Operation op_name {op_name!r}: '
                    f'{existing.__name__} and {cls.__name__}'
                )
            OPERATIONS[op_name] = cls

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
