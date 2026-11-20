"""FastAPI dependencies shared by the API routers: the API-key check and
the path-segment resolvers. A resolver turns a name into its document, or
raises 404."""

# FastAPI resolves the annotations of these functions through this module's
# namespace. See the lazy-import rules in CLAUDE.md.
__eager_imports__ = True

import secrets
from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import APIKeyHeader

from ..models.group import Group
from ..models.site import Site
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from ..queries.group import find_group_by_name
from ..queries.site import find_site
from ..queries.user import find_user_by_name, find_user_site_info

_api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)


async def require_api_key(
    request: Request, key: str | None = Security(_api_key_header),
) -> None:
    """Compare `X-API-Key` with `app.state.api_key`; no check when the
    configured key is None."""
    expected = request.app.state.api_key
    if expected is None:
        return
    if key is None or not secrets.compare_digest(key, expected):
        raise HTTPException(status_code=401,
                            detail='invalid or missing API key')


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
