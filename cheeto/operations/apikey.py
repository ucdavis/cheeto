"""Issue and revoke REST API keys.

History never holds a secret or its hash: `describe()` names the key by
its public id only.
"""

from datetime import datetime, timezone
from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..apikey import generate_api_key, normalize_scopes
from ..models.apikey import ApiKey
from ..models.user import User
from ..queries.apikey import find_api_key
from .base import Operation


class CreateApiKey(Operation):
    """Issue a key. Returns `(ApiKey, token)`; `token` is the full key for
    the client, and this return value is the only place it ever exists."""

    op_name = 'create_api_key'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        name: str,
        scopes: list[str],
        expires_at: datetime | None = None,
        comment: str = '',
    ) -> None:
        super().__init__(client, author)
        self.name = name
        self.scopes = normalize_scopes(scopes)
        self.expires_at = expires_at
        self.comment = comment
        self.key_id: str | None = None

    async def execute(self, session: AsyncClientSession) -> tuple[ApiKey, str]:
        if (self.expires_at is not None
                and self.expires_at <= datetime.now(timezone.utc)):
            raise ValueError('expires_at is not in the future')
        token, key_id, secret_hash = generate_api_key()
        key = ApiKey(
            key_id=key_id, name=self.name, secret_hash=secret_hash,
            scopes=self.scopes, comment=self.comment,
            expires_at=self.expires_at,
        )
        await key.insert(session=session)
        self.key_id = key_id
        return key, token

    def describe(self) -> dict[str, Any]:
        return {
            'key_id': self.key_id,
            'name': self.name,
            'scopes': self.scopes,
            'expires_at': (
                self.expires_at.isoformat() if self.expires_at else None
            ),
        }


class RevokeApiKey(Operation):
    """Revoke a key by its id. The API refuses it from the next request.
    The record stays. Idempotent."""

    op_name = 'revoke_api_key'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        key_id: str,
    ) -> None:
        super().__init__(client, author)
        self.key_id = key_id
        self.name: str | None = None

    async def execute(self, session: AsyncClientSession) -> ApiKey:
        key = await find_api_key(self.key_id)
        if key is None:
            raise ValueError(f'API key {self.key_id!r} does not exist')
        self.name = key.name
        if key.revoked_at is None:
            key.revoked_at = datetime.now(timezone.utc)
            await key.save(session=session)
        return key

    def describe(self) -> dict[str, Any]:
        return {'key_id': self.key_id, 'name': self.name}
