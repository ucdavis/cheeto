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


async def list_automap_storages_grouped(site: Site) -> dict[str, list[Storage]]:
    """All automount-backed Storage at `site`, bucketed by category, in one
    query (vs. one `list_automap_storages` call per category). Depth 1
    resolves volume/automount_map/owner/group — everything the LDAP/puppet
    projections read."""
    out: dict[str, list[Storage]] = {}
    storages = await Storage.find(
        Storage.site.id == site.id,
        Storage.automount_map != None,  # noqa: E711 — beanie operator quirk
        fetch_links=True,
        nesting_depth=1,
    ).to_list()
    for s in storages:
        out.setdefault(s.category, []).append(s)
    return out


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


async def find_automount_map(site: Site, name: str) -> AutomountMap | None:
    return await AutomountMap.find_one(
        AutomountMap.name == name,
        AutomountMap.site.id == site.id,
    )


async def list_site_automount_maps(site: Site) -> list[AutomountMap]:
    return await AutomountMap.find(
        AutomountMap.site.id == site.id,
    ).sort('+name').to_list()


async def list_map_storages(amap: AutomountMap) -> list[Storage]:
    """Storages (automount entries) attached to `amap`. Depth 2 resolves
    `volume` so each entry's host/host_path derive."""
    return await Storage.find(
        Storage.automount_map.id == amap.id,
        fetch_links=True,
        nesting_depth=2,
    ).sort('+name').to_list()


def mount_mechanism_label(storage: Storage) -> str:
    """Short label of a Storage's mount mechanism for display:
    `automount:<map>` / `static:<mount>` / `—`. Tolerates unfetched links
    (falls back to the bare mechanism word)."""
    if storage.automount_map is not None:
        name = getattr(storage.automount_map, 'name', None)
        return f'automount:{name}' if name else 'automount'
    if storage.static_mount is not None:
        name = getattr(storage.static_mount, 'name', None)
        return f'static:{name}' if name else 'static'
    return '—'


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
