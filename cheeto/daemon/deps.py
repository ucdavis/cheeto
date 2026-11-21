"""FastAPI dependencies shared by the API routers: the API-key check and
the path-segment resolvers. A resolver turns a name into its document, or
raises 404.

Authorization: every route declares the scopes it needs with
`dependencies=requires('slurm')`, on its router or on the route. A request
is accepted when its `X-API-Key` is

- the static `api.api_key` of the config (every scope), or
- an issued key (`cheeto ng apikey new`, stored as `models.apikey.ApiKey`)
  that is not revoked, not expired, and holds the scopes.

A route that declares no scope refuses every request: forgetting the
declaration must not open a route. Only `api.allow_anonymous: true` turns
the check off.
"""

# FastAPI resolves the annotations of these functions through this module's
# namespace. See the lazy-import rules in CLAUDE.md.
__eager_imports__ = True

import logging
import secrets
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import APIKeyHeader, SecurityScopes

from ..apikey import hash_api_secret, parse_api_key
from ..models.apikey import ApiKey
from ..models.group import Group
from ..models.site import Site
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from ..queries.apikey import find_api_key
from ..queries.group import find_group_by_name
from ..queries.site import find_site
from ..queries.user import find_user_by_name, find_user_site_info

logger = logging.getLogger(__name__)

_api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)

# Seconds between two `last_used_at` writes for one key.
KEY_TOUCH_INTERVAL = 300


class KeyTouches:
    """Decides when a key's `last_used_at` is due for a write: at most once
    per `interval` for each key. In memory, per app instance."""

    def __init__(self, interval: float = KEY_TOUCH_INTERVAL,
                 clock: Callable[[], float] = time.monotonic):
        self.interval = interval
        self.clock = clock
        self._last: dict[str, float] = {}

    def due(self, key_id: str) -> bool:
        now = self.clock()
        last = self._last.get(key_id)
        if last is not None and now - last < self.interval:
            return False
        self._last[key_id] = now
        return True


def _unauthorized() -> HTTPException:
    # One message for every cause (absent, malformed, unknown, wrong
    # secret, revoked, expired): the response must not say which.
    return HTTPException(status_code=401,
                         detail='invalid or missing API key',
                         headers={'WWW-Authenticate': 'APIKey'})


async def _touch(request: Request, key: ApiKey) -> None:
    """Record that `key` was used. The only write the API makes; it must
    never fail a request."""
    if not request.app.state.key_touches.due(key.key_id):
        return
    try:
        await ApiKey.find_one(ApiKey.key_id == key.key_id).update(
            {'$set': {'last_used_at': datetime.now(timezone.utc)}},
        )
    except Exception:
        logger.warning('could not record the last use of API key %s',
                       key.key_id, exc_info=True)


async def authorize(request: Request, scopes: Sequence[str]) -> None:
    """Accept the request for a route that needs `scopes`, or raise 401
    (no valid key) or 403 (valid key, scope not held).

    Called by the `require_api_key` dependency, and by `cache.CachedRoute`
    before it answers from the cache (the dependencies do not run on a
    cache hit). The accepted scopes are kept on `request.state`, so the
    second call in one request does no work. Never logs the key itself;
    a key id is public.
    """
    state = request.app.state
    if state.allow_anonymous:
        return
    needed = tuple(scopes)
    if getattr(request.state, 'authorized_scopes', None) == needed:
        return
    if not needed:
        logger.error('route %s declares no API scope; refusing',
                     request.scope['path'])
        raise HTTPException(status_code=403,
                            detail='this route is not available')

    token = request.headers.get('x-api-key')
    if not token:
        raise _unauthorized()

    static_key = state.api_key
    if static_key is not None and secrets.compare_digest(
        token.encode(), static_key.encode(),
    ):
        client = 'config'
    else:
        parsed = parse_api_key(token)
        key = await find_api_key(parsed[0]) if parsed is not None else None
        if key is None or not secrets.compare_digest(
            key.secret_hash, hash_api_secret(parsed[1]),
        ):
            logger.warning('refused %s: unknown API key',
                           request.scope['path'])
            raise _unauthorized()
        key_state = key.state()
        if key_state != 'active':
            logger.warning('refused %s: API key %s (%s) is %s',
                           request.scope['path'], key.key_id, key.name, key_state)
            raise _unauthorized()
        missing = [scope for scope in needed if not key.allows(scope)]
        if missing:
            logger.warning('refused %s: API key %s (%s) lacks scope %s',
                           request.scope['path'], key.key_id, key.name,
                           ', '.join(missing))
            raise HTTPException(
                status_code=403,
                detail=f'API key lacks scope: {", ".join(missing)}',
            )
        await _touch(request, key)
        client = key.name

    request.state.authorized_scopes = needed
    request.state.api_client = client


async def require_api_key(
    request: Request,
    security_scopes: SecurityScopes,
    key: str | None = Security(_api_key_header),
) -> None:
    """The dependency behind `requires(...)`. `security_scopes` are the
    scopes of the route; `key` is declared for the OpenAPI schema only."""
    await authorize(request, security_scopes.scopes)


def requires(*scopes: str) -> list:
    """`dependencies=requires('slurm')` for a router or a route."""
    return [Security(require_api_key, scopes=list(scopes))]


async def resolved_site(site: str) -> Site:
    """The `{site}` segment: a site name, an fqdn or an alias."""
    resolved = await find_site(site)
    if resolved is None:
        raise HTTPException(status_code=404, detail=f'unknown site {site!r}')
    return resolved


async def resolved_user(username: str) -> User:
    user = await find_user_by_name(username)
    if user is None:
        raise HTTPException(status_code=404,
                            detail=f'unknown user {username!r}')
    return user


async def resolved_group(groupname: str) -> Group:
    group = await find_group_by_name(groupname)
    if group is None:
        raise HTTPException(status_code=404,
                            detail=f'unknown group {groupname!r}')
    return group


@dataclass
class UserAtSite:
    user: User
    site: Site
    usi: UserSiteInfo


async def resolved_user_site(
    user: User = Depends(resolved_user),
    site: Site = Depends(resolved_site),
) -> UserAtSite:
    """A user that is present at the site (has a `UserSiteInfo`)."""
    usi = await find_user_site_info(user, site)
    if usi is None:
        raise HTTPException(
            status_code=404,
            detail=f'user {user.name!r} is not at site {site.name!r}',
        )
    return UserAtSite(user=user, site=site, usi=usi)
