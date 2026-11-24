"""Users: the global record, and the per-site views under
`/users/{username}/sites/{site}`."""

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException

from ...constants import ACCESS_TYPES, USER_STATUSES, USER_TYPES
from ...models.user import User
from ...queries.access_status import AccessStatusNames, access_status_names
from ...queries.group import user_group_roles_at_site
from ...queries.site import find_site
from ...queries.slurm import (
    resolve_slurm_account_label,
    slurm_max_jobs,
    user_slurm_at_site,
)
from ...queries.storage import list_site_storages
from ...queries.user import (
    effective_access_links,
    effective_status_link,
    find_users,
    list_user_site_infos,
    list_user_ssh_keys,
)
from ..cache import CachedRoute
from ..convert import storages_out
from ..deps import (
    UserAtSite,
    requires,
    resolved_user,
    resolved_user_site,
)
from ..schemas import (
    PartitionQOS,
    SshKeysOut,
    UserGroupOut,
    UserOut,
    UserSiteOut,
    UserSlurmAccount,
    UserSlurmOut,
    UserStorageOut,
    UserSummary,
)

router = APIRouter(prefix='/users', tags=['users'],
                   route_class=CachedRoute,
                   dependencies=requires('users'))

# The filter values are validated against the constants, so a typo is a 422
# and not an empty list.
UserType = Literal[USER_TYPES]            # type: ignore[valid-type]
UserStatus = Literal[USER_STATUSES]       # type: ignore[valid-type]
AccessType = Literal[ACCESS_TYPES]        # type: ignore[valid-type]


def _summary_fields(user: User, names: AccessStatusNames) -> dict:
    return dict(
        name=user.name, uid=user.uid, gid=user.gid, email=user.email,
        fullname=user.fullname, type=user.type,
        status=names.status_name(user.status),
        access=names.access_names(user.access),
    )


def _user_site_out(at: UserAtSite, names: AccessStatusNames) -> UserSiteOut:
    user, usi = at.user, at.usi
    return UserSiteOut(
        site=at.site.name,
        status_override=names.status_name(usi.status),
        access_override=names.access_names(usi.access),
        status=names.status_name(effective_status_link(user, usi)),
        access=names.access_names(effective_access_links(user, usi)),
        expires_at=usi.expires_at or user.expires_at,
    )


@router.get('', response_model=list[UserSummary])
async def list_users(
    type: UserType | None = None,
    status: UserStatus | None = None,
    access: AccessType | None = None,
    site: str | None = None,
    group: str | None = None,
):
    """Users that match every given filter. With `site`, the `status` and
    `access` filters use the per-site overrides and `group` counts the
    sticky groups."""
    sitename = None
    if site is not None:
        resolved = await find_site(site)
        if resolved is None:
            raise HTTPException(status_code=404,
                                detail=f'unknown site {site!r}')
        sitename = resolved.name
    users = await find_users(status=status, access=access, type=type,
                             site=sitename, group=group)
    names = await access_status_names()
    return [UserSummary(**_summary_fields(u, names)) for u in users]


@router.get('/{username}', response_model=UserOut)
async def get_user(user: User = Depends(resolved_user)):
    names = await access_status_names()
    site_rows = await list_user_site_infos(user)
    return UserOut(
        **_summary_fields(user, names),
        shell=user.shell,
        home_directory=user.home_directory,
        has_password=user.password is not None,
        expires_at=user.expires_at,
        sites=[site.name for _usi, site in site_rows],
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


@router.get('/{username}/ssh-keys', response_model=SshKeysOut)
async def user_ssh_keys(user: User = Depends(resolved_user)):
    keys = await list_user_ssh_keys(user)
    return SshKeysOut(user=user.name, keys=sorted(k.key for k in keys))


@router.get('/{username}/sites', response_model=list[UserSiteOut])
async def user_sites(user: User = Depends(resolved_user)):
    names = await access_status_names()
    return [
        _user_site_out(UserAtSite(user=user, site=site, usi=usi), names)
        for usi, site in await list_user_site_infos(user)
    ]


@router.get('/{username}/sites/{site}', response_model=UserSiteOut)
async def user_site(at: UserAtSite = Depends(resolved_user_site)):
    return _user_site_out(at, await access_status_names())


@router.get('/{username}/sites/{site}/groups',
            response_model=list[UserGroupOut])
async def user_site_groups(at: UserAtSite = Depends(resolved_user_site)):
    """The groups of the user at the site, with all explicit roles. A
    sticky group has `sticky: true` and can have no explicit role."""
    rows = await user_group_roles_at_site(at.user, at.site)
    return [
        UserGroupOut(name=r.group.name, gid=r.group.gid, roles=r.roles,
                     sticky=r.sticky)
        for r in rows
    ]


@router.get('/{username}/sites/{site}/slurm', response_model=UserSlurmOut)
async def user_site_slurm(at: UserAtSite = Depends(resolved_user_site)):
    """The Slurm accounts the user reaches at the site and the
    (partition, qos) pairs of each. `eligible` says whether the Slurm sync
    creates the associations."""
    names = await access_status_names()
    max_jobs = slurm_max_jobs(
        names.access_names(effective_access_links(at.user, at.usi)),
        names.status_name(effective_status_link(at.user, at.usi)),
    )

    accounts: dict[str, UserSlurmAccount] = {}
    for row in await user_slurm_at_site(at.user, at.site):
        account = accounts.get(row.group.name)
        if account is None:
            account = accounts[row.group.name] = UserSlurmAccount(
                account=row.group.name,
                via=[],
                associations=sorted(
                    (
                        PartitionQOS(partition=a.partition.name,
                                     qos=a.qos.name)
                        for a in row.slurm.associations
                    ),
                    key=lambda pq: (pq.partition, pq.qos),
                ),
            )
        account.via.append(row.role)

    has_association = any(a.associations for a in accounts.values())
    default_account = None
    if max_jobs is not None and has_association:
        default_account = await resolve_slurm_account_label(
            at.site.slurm.default_account,
        )
    return UserSlurmOut(
        eligible=max_jobs is not None,
        max_jobs=max_jobs,
        default_account=default_account,
        accounts=sorted(accounts.values(), key=lambda a: a.account),
    )


@router.get('/{username}/sites/{site}/storage',
            response_model=UserStorageOut)
async def user_site_storage(at: UserAtSite = Depends(resolved_user_site)):
    owned = await list_site_storages(at.site, owner_id=at.user.id)
    owned_ids = {s.id for s in owned}
    rows = await user_group_roles_at_site(at.user, at.site)
    via_groups = await list_site_storages(
        at.site, group_ids=[r.group.id for r in rows if r.is_member],
    )
    return UserStorageOut(
        owned=storages_out(owned),
        group=storages_out(s for s in via_groups if s.id not in owned_ids),
    )
