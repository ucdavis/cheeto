"""Sites, and the site-wide user and group lists (the replacement for the
per-site `all.yaml` of the puppet.hpc-accounts export)."""

from fastapi import APIRouter, Depends

from ...models.site import Site
from ...queries.group import resolve_group_names
from ...queries.site_view import (
    SiteGroupView,
    SiteUserView,
    site_groups_view,
    site_users_view,
)
from ...queries.slurm import (
    resolve_slurm_account_label,
    resolve_slurm_account_labels,
)
from ..cache import CachedRoute
from ..convert import storage_out, storages_out
from ..deps import requires, resolved_site
from ..schemas import (
    SiteDetail,
    SiteGroupOut,
    SiteSummary,
    SiteUserOut,
    SiteUserSlurm,
    UserGroupOut,
)

# No router-wide scope: the two bulk lists return user and group data, so
# they need the `users` and `groups` scopes, not `sites`. Each route
# declares its own.
router = APIRouter(prefix='/sites', tags=['sites'],
                   route_class=CachedRoute)


def _site_user_out(v: SiteUserView) -> SiteUserOut:
    u = v.user
    return SiteUserOut(
        name=u.name, uid=u.uid, gid=u.gid, email=u.email,
        fullname=u.fullname, type=u.type, shell=u.shell,
        home_directory=u.home_directory,
        has_password=u.password is not None,
        status=v.status, access=v.access,
        expires_at=v.usi.expires_at or u.expires_at,
        groups=[
            UserGroupOut(name=g.name, gid=g.gid, roles=g.roles,
                         sticky=g.sticky)
            for g in v.groups
        ],
        slurm=SiteUserSlurm(
            eligible=v.slurm_max_jobs is not None,
            max_jobs=v.slurm_max_jobs,
            accounts=v.slurm_accounts,
        ),
        home=storage_out(v.home) if v.home is not None else None,
    )


def _site_group_out(v: SiteGroupView) -> SiteGroupOut:
    return SiteGroupOut(
        name=v.group.name, gid=v.group.gid, type=v.group.type,
        sticky=v.sticky, slurm_account=v.slurm_account,
        members=v.members, sponsors=v.sponsors, sudoers=v.sudoers,
        slurmers=v.slurmers, storages=storages_out(v.storages),
    )


@router.get('', response_model=list[SiteSummary],
            dependencies=requires('sites'))
async def list_sites():
    sites = await Site.find_all().sort('+name').to_list()
    return [
        SiteSummary(name=s.name, fqdn=s.fqdn, aliases=s.aliases)
        for s in sites
    ]


@router.get('/{site}', response_model=SiteDetail,
            dependencies=requires('sites'))
async def get_site(site: Site = Depends(resolved_site)):
    return SiteDetail(
        name=site.name, fqdn=site.fqdn, aliases=site.aliases,
        sticky_groups=await resolve_group_names(site.group.sticky),
        sticky_slurm_accounts=await resolve_slurm_account_labels(
            site.slurm.sticky,
        ),
        default_slurm_account=await resolve_slurm_account_label(
            site.slurm.default_account,
        ),
    )


@router.get('/{site}/users', response_model=list[SiteUserOut],
            dependencies=requires('users'))
async def site_users(site: Site = Depends(resolved_site)):
    """Every user at the site, with the effective status and access, the
    groups and roles, the Slurm accounts and the home storage. The query
    count does not depend on the number of users."""
    return [_site_user_out(v) for v in await site_users_view(site)]


@router.get('/{site}/groups', response_model=list[SiteGroupOut],
            dependencies=requires('groups'))
async def site_groups(site: Site = Depends(resolved_site),
                      personal: bool = False):
    """Every group present at the site, with its roster and storages.
    `personal=true` also returns the per-user groups."""
    return [
        _site_group_out(v)
        for v in await site_groups_view(site, personal=personal)
    ]
