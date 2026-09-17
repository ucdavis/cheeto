"""Storage-related read-only query helpers.

Pure read functions used by the LDAP sync operations and the storage CLI.

Depth guide: `nesting_depth=1` resolves `Storage.volume` and
`Storage.automount_map` — enough for host/host_path/quota/mount_options and
automount mount_path (volume host/host_path are concrete, never resolved
through `parent`). Static-mount `mount_path` additionally needs
`static_mount.volume` fetched, i.e. `nesting_depth=2` — only `get_storage`
uses that. The bulk listing (`list_site_storages`) instead resolves links
with one batched query per field (`_hydrate_storage_links`): a blanket
`nesting_depth=2` find fans every owner out into its status/access/back-links
and is pathologically slow on a busy site.
"""

from __future__ import annotations

import asyncio

from beanie import PydanticObjectId
from beanie.operators import In

from ..models.base import link_target_id
from ..models.group import Group
from ..models.host import StorageHost
from ..models.site import Site, SiteStorageSettings
from ..models.storage import (
    AutomountMap,
    NFSExportConfig,
    StaticMount,
    Storage,
    StorageVolume,
)
from ..models.user import User


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
    """One volume with its direct links (`site`, `parent`, `storage_host`)
    fetched — the `show volume` read."""
    return await StorageVolume.find_one(
        StorageVolume.name == name,
        StorageVolume.site.id == site.id,
        fetch_links=True,
        nesting_depth=1,
    )


# ---------------------------------------------------------------------------
# Storage hosts + default resolution
# ---------------------------------------------------------------------------
#
# Reads resolve a volume's StorageHost BY NAME through the unique
# `(site, hostname)` key rather than by fetching `StorageVolume.storage_host`:
# `host` is denormalized on every volume, so no extra link depth is needed
# and rows the backfill has not linked yet resolve the same way.


async def find_storage_host(site: Site, hostname: str) -> StorageHost | None:
    # StorageHost.find_one auto-filters `_class_id`; never resolve through
    # the `Host` root (see models/host.py).
    return await StorageHost.find_one(
        StorageHost.hostname == hostname,
        StorageHost.site.id == site.id,
    )


async def list_site_storage_hosts(site: Site) -> list[StorageHost]:
    return await StorageHost.find(
        StorageHost.site.id == site.id,
    ).sort('+hostname').to_list()


async def storage_hosts_by_site(
    site_ids,
) -> dict[tuple[PydanticObjectId, str], StorageHost]:
    """Every StorageHost at the given sites keyed `(site_id, hostname)`, in
    one query — the lookup table bulk readers (puppet export) resolve a
    volume's host through."""
    ids = list(site_ids)
    if not ids:
        return {}
    hosts = await StorageHost.find(In(StorageHost.site.id, ids)).to_list()
    return {(link_target_id(h.site), h.hostname): h for h in hosts}


async def volume_counts_by_host(site: Site) -> dict[str, int]:
    """`{hostname: volume count}` for a site in one `$group` aggregation."""
    rows = await StorageVolume.find(
        StorageVolume.site.id == site.id,
    ).aggregate([
        {'$group': {'_id': '$host', 'n': {'$sum': 1}}},
    ]).to_list()
    return {row['_id']: row['n'] for row in rows}


NFS_EXPORT_LEVELS = ('storage', 'volume', 'host', 'site')


def effective_nfs_export(
    *,
    volume: StorageVolume | None,
    host: StorageHost | None,
    site: Site | None,
    storage: Storage | None = None,
) -> tuple[NFSExportConfig | None, dict[str, str]]:
    """Resolve the export config a puppet `/etc/exports` line should carry.

    Per field, first non-empty value wins along
    `storage` -> `volume` -> `host` -> `site`. Per-field (not whole-config)
    because a volume created with only `--export-ranges` stores
    `export_options=''` and must still take options from the site. No range
    union: a more specific tier can restrict ranges. `host` and `site` are
    the VOLUME's (a Storage may sit on another site than its volume).

    Returns `(config, levels)` where `levels` maps each field to the tier
    it came from (`'none'` when nothing set it); `config` is None only when
    both fields are unset everywhere. `volume=None` resolves the host/site
    default alone (what a volume with no config of its own would inherit).
    """
    tiers = (
        ('storage', storage.nfs_export if storage is not None else None),
        ('volume', volume.nfs_export if volume is not None else None),
        ('host', host.nfs_export if host is not None else None),
        ('site', site.storage.nfs_export if site is not None else None),
    )
    options, options_level = '', 'none'
    ranges: list[str] = []
    ranges_level = 'none'
    for level, cfg in tiers:
        if cfg is None:
            continue
        if options_level == 'none' and cfg.export_options:
            options, options_level = cfg.export_options, level
        if ranges_level == 'none' and cfg.export_ranges:
            ranges, ranges_level = list(cfg.export_ranges), level
    levels = {'export_options': options_level, 'export_ranges': ranges_level}
    if options_level == 'none' and ranges_level == 'none':
        return None, levels
    return NFSExportConfig(export_options=options, export_ranges=ranges), levels


def effective_zfs_path_template(
    category: str, *, host: StorageHost | None, site: Site | None,
) -> tuple[str | None, str]:
    """The ZFS path template for `category`: host tier first, then site.
    Returns `(template, level)` with level `host` / `site` / `none`."""
    if host is not None:
        template = host.zfs_path_templates.get(category)
        if template:
            return template, 'host'
    if site is not None:
        template = site.storage.zfs_path_templates.get(category)
        if template:
            return template, 'site'
    return None, 'none'


async def list_site_volumes(site: Site) -> list[StorageVolume]:
    """All volumes at a site (sorted by name), each `parent` resolved from the
    in-result set rather than via a link fetch — volumes nest within a site,
    so the parent is always present in the same result. Callers that only need
    the parent id (e.g. `link_target_id(v.parent)`) work either way."""
    volumes = await StorageVolume.find(
        StorageVolume.site.id == site.id,
    ).sort('+name').to_list()
    by_id = {v.id: v for v in volumes}
    for v in volumes:
        pid = link_target_id(v.parent)
        if pid is not None and pid in by_id:
            v.parent = by_id[pid]
    return volumes


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
    """Resolve a site's storage defaults to display values: the default home
    volume / automount map / static mount names, the plain default home
    quota string, and the site-tier export config and ZFS path templates.
    One lightweight fetch per set ref."""
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
        'nfs_export': (
            settings.nfs_export.model_dump()
            if settings.nfs_export is not None else None
        ),
        'zfs_path_templates': dict(settings.zfs_path_templates),
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


async def _hydrate_storage_links(storages: list[Storage]) -> None:
    """Resolve every storage's Link fields with one batched query per field
    and assign the fetched docs back in place, so the derived properties
    (host/host_path/quota/mount_path/mount_options) work without a deep
    per-row fetch. `static_mount` is fetched with its own volume so static
    `mount_path` resolves. Fixed query count, independent of len(storages)."""
    if not storages:
        return

    def _ids(attr: str) -> list:
        return list({
            tid for s in storages
            if (tid := link_target_id(getattr(s, attr))) is not None
        })

    async def _by_id(model, ids, **find_kwargs) -> dict:
        if not ids:
            return {}
        docs = await model.find(In(model.id, ids), **find_kwargs).to_list()
        return {d.id: d for d in docs}

    volumes, owners, groups, maps, smounts = await asyncio.gather(
        _by_id(StorageVolume, _ids('volume')),
        _by_id(User, _ids('owner')),
        _by_id(Group, _ids('group'), with_children=True),
        _by_id(AutomountMap, _ids('automount_map')),
        _by_id(StaticMount, _ids('static_mount'),
               fetch_links=True, nesting_depth=1),
    )
    for s in storages:
        s.volume = volumes.get(link_target_id(s.volume))
        s.owner = owners.get(link_target_id(s.owner))
        s.group = groups.get(link_target_id(s.group))
        mid = link_target_id(s.automount_map)
        s.automount_map = maps.get(mid) if mid is not None else None
        sid = link_target_id(s.static_mount)
        s.static_mount = smounts.get(sid) if sid is not None else None


async def list_site_storages(
    site: Site, category: str | None = None, *,
    owner_id=None, group_id=None, host=None,
) -> list[Storage]:
    """All Storage records at a site, with volume/owner/group/mount links
    resolved via batched per-field queries (`_hydrate_storage_links`) so the
    derived properties work. Optionally filtered (AND) by category and/or
    owner/group document id (indexed Storage fields), and/or backing-volume
    host (a derived property, filtered on the hydrated rows)."""
    filters = [Storage.site.id == site.id]
    if category is not None:
        filters.append(Storage.category == category)
    if owner_id is not None:
        filters.append(Storage.owner.id == owner_id)
    if group_id is not None:
        filters.append(Storage.group.id == group_id)
    storages = await Storage.find(*filters).sort('+name').to_list()
    await _hydrate_storage_links(storages)
    if host is not None:
        storages = [s for s in storages if s.host == host]
    return storages
