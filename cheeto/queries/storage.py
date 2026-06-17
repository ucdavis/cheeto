"""Storage-related read-only query helpers.

Pure read functions used by the LDAP sync operations and the storage CLI.

Depth guide: `nesting_depth=1` resolves `Storage.volume` and
`Storage.automount_map` — enough for host/host_path/quota/mount_options and
automount mount_path (volume host/host_path are concrete, never resolved
through `parent`). Static-mount `mount_path` additionally needs
`static_mount.volume` fetched, i.e. `nesting_depth=2` — only `get_storage`
and the CLI listing use that.
"""

from __future__ import annotations

import asyncio

from ..models.base import link_target_id
from ..models.site import Site, SiteStorageSettings
from ..models.storage import AutomountMap, StaticMount, Storage, StorageVolume


async def list_automap_storages(site: Site, category: str) -> list[Storage]:
    """List Storage rows at `site` whose category matches and that have an
    automount_map (i.e. are intended to be projected into LDAP autofs).

    `category` is one of 'home' / 'group' (matching v1's
    `query_automap_storages` semantics). Depth 1 resolves `volume` and
    `automount_map`, which is all the LDAP projection needs."""
    return await Storage.find(
        Storage.site.id == site.id,
        Storage.category == category,
        Storage.automount_map != None,  # noqa: E711 — beanie operator quirk
        fetch_links=True,
        nesting_depth=1,
    ).to_list()


async def find_volume(site: Site, name: str) -> StorageVolume | None:
    return await StorageVolume.find_one(
        StorageVolume.name == name,
        StorageVolume.site.id == site.id,
    )


async def list_site_volumes(site: Site) -> list[StorageVolume]:
    return await StorageVolume.find(
        StorageVolume.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    ).sort('+name').to_list()


async def find_static_mount(site: Site, name: str) -> StaticMount | None:
    return await StaticMount.find_one(
        StaticMount.name == name,
        StaticMount.site.id == site.id,
    )


async def list_site_static_mounts(site: Site) -> list[StaticMount]:
    """Depth 1 resolves `.volume`, enabling `device_spec`/`host_path`."""
    return await StaticMount.find(
        StaticMount.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    ).sort('+mount_path').to_list()


async def _resolve_storage_name(ref, model) -> str | None:
    """Resolve a SiteStorageSettings DocRef (bare ObjectId or None) to the
    referenced document's `name`, or None if unset/missing."""
    target_id = link_target_id(ref)
    if target_id is None:
        return None
    doc = await model.get(target_id)
    return doc.name if doc is not None else None


async def resolve_site_storage_settings(settings: SiteStorageSettings) -> dict:
    """Resolve a site's home-storage provisioning defaults to display labels:
    the default home volume / automount map / static mount names, plus the
    plain default home quota string. One lightweight fetch per set ref."""
    volume, automount, static = await asyncio.gather(
        _resolve_storage_name(settings.default_home_volume, StorageVolume),
        _resolve_storage_name(settings.home_automount_map, AutomountMap),
        _resolve_storage_name(settings.home_static_mount, StaticMount),
    )
    return {
        'default_home_volume': volume,
        'default_home_quota': settings.default_home_quota,
        'home_automount_map': automount,
        'home_static_mount': static,
    }


async def get_storage(
    site: Site, name: str, category: str | None = None,
) -> Storage | None:
    """Fetch one Storage with everything its derived properties need —
    including static-mount `mount_path` (depth 2 resolves
    `static_mount.volume`)."""
    filters = [
        Storage.name == name,
        Storage.site.id == site.id,
    ]
    if category is not None:
        filters.append(Storage.category == category)
    return await Storage.find_one(
        *filters,
        fetch_links=True,
        nesting_depth=2,
    )


async def list_site_storages(
    site: Site, category: str | None = None,
) -> list[Storage]:
    """All Storage records at a site, fully resolved (depth 2) so
    mount_path works for both automount and static mounts."""
    filters = [Storage.site.id == site.id]
    if category is not None:
        filters.append(Storage.category == category)
    return await Storage.find(
        *filters,
        fetch_links=True,
        nesting_depth=2,
    ).sort('+name').to_list()


async def list_admin_root_ssh_keys(site: Site) -> list[str]:
    """Collect SSH keys from active admin users on `site` that have
    'root-ssh' access. Used by SyncUserToLDAP for `type='system'` users
    so admins can ssh-as-root via the system account."""
    from ..models.group import AccessGroup
    from ..models.user import SshKey, User
    from ..models.user_site_info import UserSiteInfo

    root_ssh = await AccessGroup.find_one(
        AccessGroup.access_name == 'root-ssh',
    )
    if root_ssh is None:
        return []

    # Admin users on this site with root-ssh access (global or per-site).
    admins = await User.find(
        User.type == 'admin',
        User.access.id == root_ssh.id,
    ).to_list()
    if not admins:
        return []

    on_site_ids: set = set()
    for admin in admins:
        usi = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == admin.id,
            UserSiteInfo.site.id == site.id,
        )
        if usi is not None:
            on_site_ids.add(admin.id)

    if not on_site_ids:
        return []

    keys: list[str] = []
    for admin in admins:
        if admin.id not in on_site_ids:
            continue
        admin_keys = await SshKey.find(SshKey.user.id == admin.id).to_list()
        keys.extend(k.key for k in admin_keys)
    return keys
