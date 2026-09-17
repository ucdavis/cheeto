from __future__ import annotations

import decimal
from pathlib import PurePosixPath
from typing import Any

from beanie import Link
from beanie.operators import In
from pydantic import ValidationError
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import DuplicateKeyError

from ..constants import STORAGE_CATEGORIES
from ..models.base import link_target_id
from ..models.group import Group
from ..models.group_membership import GroupMembership
from ..models.host import StorageHost
from ..models.site import Site
from ..models.storage import (
    AutomountMap,
    MountOverrides,
    NFSExportConfig,
    QuobyteConfig,
    StaticMount,
    Storage,
    StorageAllocation,
    StorageVolume,
    ZFSConfig,
    _join_host_path,
)
from ..models.storage_defaults import (
    StorageDefaults,
    render_host_path,
    validate_zfs_path_templates,
)
from ..models.user import User
from ..queries.storage import (
    effective_nfs_export,
    effective_zfs_path_template,
    find_storage_host,
    get_storage,
    list_site_volumes,
    storage_hosts_by_site,
)
from ..utils import size_to_megs_exact
from .base import UNSET, Operation
from .group_site import ensure_group_site


def _backend_config_kwargs(backend: str) -> dict[str, Any]:
    """Default embedded config for a backend (the validator forbids carrying
    the other backend's config)."""
    if backend == 'zfs':
        return {'zfs': ZFSConfig()}
    return {'quobyte': QuobyteConfig()}


async def _find_site(site_name: str) -> Site:
    site = await Site.find_one(Site.name == site_name)
    if site is None:
        raise ValueError(f'Site {site_name} does not exist')
    return site


async def _find_volume(site: Site, name: str) -> StorageVolume | None:
    return await StorageVolume.find_one(
        StorageVolume.name == name,
        StorageVolume.site.id == site.id,
    )


def _make_allocation(quota: str, comment: str = '') -> StorageAllocation:
    """Construct a validated StorageAllocation. The model's quota pattern is
    unanchored (e.g. '10X' slips through) and pydantic raises ValidationError
    rather than ValueError, so we also parse via size_to_megs_exact and
    normalize every failure to a clean ValueError the CLI can render."""
    try:
        alloc = StorageAllocation(quota=quota, comment=comment)
        size_to_megs_exact(quota)
    except (ValidationError, ValueError, decimal.InvalidOperation):
        raise ValueError(
            f'Invalid quota {quota!r}; expected a size like 1T, 500G, 10P'
        )
    return alloc


# ---------------------------------------------------------------------------
# Storage hosts + storage defaults (shared by volume, host, and site ops)
# ---------------------------------------------------------------------------


def _normalize_ranges(ranges) -> list[str]:
    # Stored sorted + de-duplicated so tier comparisons (backfill dedupe) and
    # puppet output are order-independent.
    return sorted(set(ranges or []))


def apply_nfs_export(
    target: Any,
    *,
    export_options: Any = UNSET,
    export_ranges: Any = UNSET,
    clear: bool = False,
) -> dict[str, Any]:
    """Edit `target.nfs_export` in place with UNSET semantics: a kwarg left
    UNSET is untouched; `clear=True` nulls the whole config (exclusive with
    the field kwargs). Passing only one field creates the config with the
    other left empty — the per-field precedence in `effective_nfs_export`
    fills the gap from the next tier. Returns the describe() fragment."""
    if clear and (export_options is not UNSET or export_ranges is not UNSET):
        raise ValueError(
            'clear_nfs_export is exclusive with export_options / export_ranges'
        )
    changes: dict[str, Any] = {}
    if clear:
        target.nfs_export = None
        changes['nfs_export'] = None
        return changes
    if export_options is UNSET and export_ranges is UNSET:
        return changes
    cfg = target.nfs_export or NFSExportConfig()
    if export_options is not UNSET:
        cfg.export_options = export_options or ''
        changes['export_options'] = cfg.export_options
    if export_ranges is not UNSET:
        cfg.export_ranges = _normalize_ranges(export_ranges)
        changes['export_ranges'] = list(cfg.export_ranges)
    target.nfs_export = cfg
    return changes


def apply_storage_defaults(
    target: StorageDefaults,
    *,
    export_options: Any = UNSET,
    export_ranges: Any = UNSET,
    clear_nfs_export: bool = False,
    set_templates: dict[str, str] | None = None,
    unset_templates: list[str] | None = None,
) -> dict[str, Any]:
    """Edit a `StorageDefaults` tier (`Site.storage` or a `StorageHost`) in
    place: the export config via `apply_nfs_export`, then remove and add ZFS
    path templates by category. Templates are validated here so a bad one is
    a clean ValueError before any save. Returns the describe() fragment."""
    changes = apply_nfs_export(
        target, export_options=export_options, export_ranges=export_ranges,
        clear=clear_nfs_export,
    )
    if unset_templates:
        for category in unset_templates:
            if category not in STORAGE_CATEGORIES:
                raise ValueError(
                    f'Invalid ZFS path template category {category!r}; '
                    f'expected one of {", ".join(STORAGE_CATEGORIES)}'
                )
        target.zfs_path_templates = {
            k: v for k, v in target.zfs_path_templates.items()
            if k not in set(unset_templates)
        }
        changes['unset_zfs_path_templates'] = list(unset_templates)
    if set_templates:
        validate_zfs_path_templates(set_templates)
        target.zfs_path_templates = {
            **target.zfs_path_templates, **set_templates,
        }
        changes['zfs_path_templates'] = dict(set_templates)
    return changes


async def _resolve_storage_host(site: Site, hostname: str) -> StorageHost:
    """Strict: the StorageHost `hostname` names at `site`, or a ValueError
    pointing at `ng storage new host`. Catches typos in `--host` escape
    hatches; after `ng storage backfill-hosts` every live host has a record."""
    host = await find_storage_host(site, hostname)
    if host is None:
        raise ValueError(
            f'StorageHost {hostname!r} does not exist on {site.name}; create '
            f'it with `ng storage new host {hostname} --site {site.name}`'
        )
    return host


async def _inherit_storage_host(parent: StorageVolume):
    """Lenient host link for a child volume: the parent's own link when set,
    else the StorageHost named by `parent.host` at the parent's site, else
    None. Never raises — home provisioning under a pre-backfill parent must
    keep working."""
    if parent.storage_host is not None:
        return parent.storage_host
    site_id = link_target_id(parent.site)
    if site_id is None:
        return None
    return await StorageHost.find_one(
        StorageHost.hostname == parent.host,
        StorageHost.site.id == site_id,
    )


def _standalone_host_path(
    site: Site,
    host: StorageHost,
    *,
    category: str,
    name: str,
    explicit: str | None,
    fallback: str | None,
) -> str:
    """Host path for a standalone (parent-less) volume: the explicit path if
    given, else the host/site ZFS path template for `category` rendered with
    `{host}`/`{name}`/`{site}`, else `fallback`. Children never come through
    here — they take `parent.host_path/<name>`."""
    if explicit:
        return explicit
    template, _level = effective_zfs_path_template(
        category, host=host, site=site,
    )
    if template:
        return render_host_path(
            template, host=host.hostname, name=name, site=site.name,
        )
    if fallback is not None:
        return fallback
    raise ValueError(
        f'No ZFS path template for category {category!r} on {site.name} '
        f'(host {host.hostname}); pass host_path/--host-path, or set one '
        f'with `ng site set storage-defaults --zfs-path-template '
        f'{category}=/{{host}}/{category}/{{name}} --site {site.name}` (or '
        f'`ng storage edit host {host.hostname}` for a host-specific one)'
    )


class CreateStorageVolume(Operation):
    """Create a StorageVolume record — the provisionable backing entity (a
    ZFS dataset or QuoByte volume). Does not (yet) provision anything on
    the backend itself.

    `host` must name an existing StorageHost at the site (strict; see
    `_resolve_storage_host`). `host_path` may be omitted when `template`
    names a storage category whose ZFS path template resolves on the host or
    site; `{name}` renders as the volume name's last path component
    (`group/foo` -> `foo`). An explicit `host_path` always wins. Export
    config is stored only when given — otherwise the volume inherits the
    host/site defaults through `effective_nfs_export`.
    """

    op_name = 'create_storage_volume'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        name: str,
        backend: str,
        host: str,
        host_path: str | None = None,
        template: str | None = None,
        parent_name: str | None = None,
        quota: str | None = None,
        export_options: str | None = None,
        export_ranges: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        if host_path is None and template is None:
            raise ValueError(
                'CreateStorageVolume requires host_path or a template '
                'category (pass --host-path or --template)'
            )
        if template is not None and template not in STORAGE_CATEGORIES:
            raise ValueError(
                f'Invalid template category {template!r}; expected one of '
                f'{", ".join(STORAGE_CATEGORIES)}'
            )
        self.site_name = site_name
        self.name = name
        self.backend = backend
        self.host = host
        self.host_path = host_path
        self.template = template
        self.parent_name = parent_name
        self.quota = quota
        self.export_options = export_options
        self.export_ranges = export_ranges
        self._resolved_host_path = host_path

    async def execute(self, session: AsyncClientSession) -> StorageVolume:
        site = await _find_site(self.site_name)

        existing = await _find_volume(site, self.name)
        if existing is not None:
            raise ValueError(
                f'StorageVolume {self.name} already exists on {self.site_name}'
            )

        parent = None
        if self.parent_name is not None:
            parent = await _find_volume(site, self.parent_name)
            if parent is None:
                raise ValueError(
                    f'Parent volume {self.parent_name} does not exist on '
                    f'{self.site_name}'
                )

        host = await _resolve_storage_host(site, self.host)
        host_path = _standalone_host_path(
            site, host,
            category=self.template or '',
            name=PurePosixPath(self.name).name,
            explicit=self.host_path,
            fallback=None,
        )
        self._resolved_host_path = host_path

        nfs_export = None
        if self.export_options or self.export_ranges:
            nfs_export = NFSExportConfig(
                export_options=self.export_options or '',
                export_ranges=_normalize_ranges(self.export_ranges),
            )

        volume = StorageVolume(
            name=self.name,
            site=site,
            backend=self.backend,
            host=host.hostname,
            host_path=host_path,
            storage_host=host,
            parent=parent,
            allocations=(
                [_make_allocation(self.quota, 'initial allocation')]
                if self.quota else []
            ),
            nfs_export=nfs_export,
            **_backend_config_kwargs(self.backend),
        )
        await volume.insert(session=session)
        self._volume = volume
        return volume

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'name': self.name,
            'backend': self.backend,
            'host': self.host,
            'host_path': self._resolved_host_path,
            'template': self.template,
            'parent': self.parent_name,
            'quota': self.quota,
            'export_options': self.export_options,
            'export_ranges': (
                _normalize_ranges(self.export_ranges)
                if self.export_ranges else None
            ),
        }


class CreateStaticMount(Operation):
    """Create a StaticMount record — an fstab-style mount at a fixed path
    on a static-mount cluster (e.g. Hive)."""

    op_name = 'create_static_mount'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        name: str,
        fstype: str,
        mount_path: str,
        volume_name: str | None = None,
        subpath: str = '',
        spec: str = '',
        options: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.name = name
        self.fstype = fstype
        self.mount_path = mount_path
        self.volume_name = volume_name
        self.subpath = subpath
        self.spec = spec
        self.options = options or []

    async def execute(self, session: AsyncClientSession) -> StaticMount:
        site = await _find_site(self.site_name)

        existing = await StaticMount.find_one(
            StaticMount.name == self.name,
            StaticMount.site.id == site.id,
        )
        if existing is not None:
            raise ValueError(
                f'StaticMount {self.name} already exists on {self.site_name}'
            )

        volume = None
        if self.volume_name is not None:
            volume = await _find_volume(site, self.volume_name)
            if volume is None:
                raise ValueError(
                    f'Volume {self.volume_name} does not exist on '
                    f'{self.site_name}'
                )

        mount = StaticMount(
            name=self.name,
            site=site,
            fstype=self.fstype,
            volume=volume,
            subpath=self.subpath,
            spec=self.spec,
            mount_path=self.mount_path,
            options=self.options,
        )
        await mount.insert(session=session)
        self._mount = mount
        return mount

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'name': self.name,
            'fstype': self.fstype,
            'mount_path': self.mount_path,
            'volume': self.volume_name,
            'spec': self.spec,
        }


async def _resolve_home_parent(
    site: Site, parent_volume_name: str | None,
) -> StorageVolume:
    if parent_volume_name is not None:
        parent = await _find_volume(site, parent_volume_name)
        if parent is None:
            raise ValueError(
                f'Parent volume {parent_volume_name} does not exist on '
                f'{site.name}'
            )
        return parent
    default_id = link_target_id(site.storage.default_home_volume)
    if default_id is None:
        raise ValueError(
            f'Site {site.name} has no default home volume; pass '
            f'parent_volume/--parent-volume or host/--host, or set one '
            f'with `ng site set storage-defaults`'
        )
    parent = await StorageVolume.get(default_id)
    if parent is None:
        raise ValueError(f'Site {site.name} default home volume is dangling')
    return parent


async def _build_home_volume(
    site: Site, user_name: str, *,
    quota: str | None, parent_volume_name: str | None,
    host: str | None, host_path: str | None,
) -> StorageVolume:
    if host is not None:
        host_doc = await _resolve_storage_host(site, host)
        return StorageVolume(
            name=f'home/{user_name}',
            site=site,
            backend='zfs',
            host=host_doc.hostname,
            host_path=_standalone_host_path(
                site, host_doc, category='home', name=user_name,
                explicit=host_path, fallback=f'/home/{user_name}',
            ),
            storage_host=host_doc,
            allocations=(
                [_make_allocation(quota, 'initial home allocation')]
                if quota else []
            ),
            **_backend_config_kwargs('zfs'),
        )

    parent = await _resolve_home_parent(site, parent_volume_name)
    quota = quota or site.storage.default_home_quota
    return StorageVolume(
        name=f'{parent.name}/{user_name}',
        site=site,
        backend=parent.backend,
        host=parent.host,
        host_path=_join_host_path(parent.host_path, user_name),
        storage_host=await _inherit_storage_host(parent),
        parent=parent,
        allocations=(
            [_make_allocation(quota, 'initial home allocation')]
            if quota else []
        ),
        **_backend_config_kwargs(parent.backend),
    )


async def _resolve_home_mount(
    site: Site, *,
    automount_map: str | None, static_mount: str | None, no_mount: bool,
) -> tuple[AutomountMap | None, StaticMount | None]:
    if no_mount:
        return None, None
    if automount_map is not None:
        amap = await AutomountMap.find_one(
            AutomountMap.name == automount_map,
            AutomountMap.site.id == site.id,
        )
        if amap is None:
            raise ValueError(
                f'AutomountMap {automount_map} does not exist on {site.name}'
            )
        return amap, None
    if static_mount is not None:
        smount = await StaticMount.find_one(
            StaticMount.name == static_mount,
            StaticMount.site.id == site.id,
        )
        if smount is None:
            raise ValueError(
                f'StaticMount {static_mount} does not exist on {site.name}'
            )
        return None, smount

    amap_id = link_target_id(site.storage.home_automount_map)
    if amap_id is not None:
        return await AutomountMap.get(amap_id), None
    smount_id = link_target_id(site.storage.home_static_mount)
    if smount_id is not None:
        smount = await StaticMount.find_one(
            StaticMount.id == smount_id, fetch_links=True, nesting_depth=1,
        )
        return None, smount

    # Legacy fallback: a map conventionally named 'home'.
    amap = await AutomountMap.find_one(
        AutomountMap.name == 'home',
        AutomountMap.site.id == site.id,
    )
    return amap, None


async def _provision_home_storage(
    session: AsyncClientSession, *,
    site: Site, user: User, group: Group,
    quota: str | None = None, parent_volume: str | None = None,
    automount_map: str | None = None, static_mount: str | None = None,
    no_mount: bool = False, host: str | None = None,
    host_path: str | None = None,
) -> tuple[Storage, str]:
    """Build + insert the home volume and the user-facing Storage from the
    given options, returning (storage, mechanism_label). Shared by
    CreateHomeStorage and RehomeUser; the caller owns any pre-existing-home
    guard. The volume insert is pre-checked so a name clash surfaces as a
    clean ValueError rather than a DuplicateKeyError."""
    volume = await _build_home_volume(
        site, user.name, quota=quota, parent_volume_name=parent_volume,
        host=host, host_path=host_path,
    )
    if await _find_volume(site, volume.name) is not None:
        raise ValueError(
            f'StorageVolume {volume.name} already exists on {site.name}'
        )
    await volume.insert(session=session)

    amap, smount = await _resolve_home_mount(
        site, automount_map=automount_map, static_mount=static_mount,
        no_mount=no_mount,
    )
    mechanism = 'none'
    if amap is not None:
        mechanism = f'automount:{amap.name}'
    elif smount is not None:
        mechanism = f'static:{smount.mount_path}'

    storage = Storage(
        name=user.name,
        site=site,
        category='home',
        owner=user,
        group=group,
        volume=volume,
        subpath='',
        automount_map=amap,
        mount_name=user.name if amap is not None else '',
        static_mount=smount,
    )
    await storage.insert(session=session)
    # The home's personal group must be present at the site.
    await ensure_group_site(group, site, session)
    return storage, mechanism


class CreateHomeStorage(Operation):
    """Provision a user's home storage: a child StorageVolume under the
    site's default home volume (or an explicit parent / standalone host)
    plus the user-facing Storage record with the site's home mount
    mechanism.

    Volume resolution, in priority order:
      1. `host` given → standalone volume (escape hatch; `host_path` or
         `/home/<user>`).
      2. `parent_volume` named, else `site.storage.default_home_volume` →
         child volume `'{parent.name}/{user}'` carved under the parent
         (backend/host inherited, host_path = parent.host_path/<user>).
    Quota: `quota` arg, else `site.storage.default_home_quota`.

    Mount resolution, in priority order: explicit `automount_map` /
    `static_mount` name → site settings (`home_automount_map` /
    `home_static_mount`) → legacy fallback: AutomountMap named 'home' at
    the site → no mount. `no_mount=True` skips entirely.
    """

    op_name = 'create_home_storage'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        user_name: str,
        site_name: str,
        quota: str | None = None,
        parent_volume: str | None = None,
        automount_map: str | None = None,
        static_mount: str | None = None,
        no_mount: bool = False,
        host: str | None = None,
        host_path: str | None = None,
    ) -> None:
        super().__init__(client, author)
        if automount_map is not None and static_mount is not None:
            raise ValueError(
                'automount_map and static_mount are mutually exclusive'
            )
        self.user_name = user_name
        self.site_name = site_name
        self.quota = quota
        self.parent_volume = parent_volume
        self.automount_map = automount_map
        self.static_mount = static_mount
        self.no_mount = no_mount
        self.host = host
        self.host_path = host_path
        self._mechanism = 'none'

    async def execute(self, session: AsyncClientSession) -> Storage:
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        group = await Group.find_one(Group.name == self.user_name)
        if group is None:
            raise ValueError(f'Group {self.user_name} does not exist')

        site = await _find_site(self.site_name)

        existing = await Storage.find_one(
            Storage.name == self.user_name,
            Storage.site.id == site.id,
            Storage.category == 'home',
        )
        if existing is not None:
            raise ValueError(
                f'Home storage for {self.user_name} on {self.site_name} '
                f'already exists'
            )

        storage, mechanism = await _provision_home_storage(
            session, site=site, user=user, group=group,
            quota=self.quota, parent_volume=self.parent_volume,
            automount_map=self.automount_map, static_mount=self.static_mount,
            no_mount=self.no_mount, host=self.host, host_path=self.host_path,
        )
        self._mechanism = mechanism
        self._storage = storage
        return storage

    def describe(self) -> dict[str, Any]:
        return {
            'user': self.user_name,
            'site': self.site_name,
            'quota': self.quota,
            'parent_volume': self.parent_volume,
            'host': self.host,
            'mechanism': self._mechanism,
        }


async def _build_group_volume(
    site: Site, group_name: str, *,
    quota: str, parent_volume_name: str | None,
    host: str | None, host_path: str | None,
    comment: str | None = None,
) -> StorageVolume:
    """Build (not insert) the backing StorageVolume for a group's storage.

    Standalone on `host` (named 'group/<group>'), else a child carved under an
    explicitly-named parent volume (named '<parent>/<group>', backend/host
    inherited). There is no site group-default, so a parent volume or host is
    required. The volume carries the group's quota as its sole allocation,
    labelled `comment` (default 'initial group allocation').
    """
    alloc = _make_allocation(
        quota,
        comment if comment is not None else 'initial group allocation',
    )
    if host is not None:
        host_doc = await _resolve_storage_host(site, host)
        return StorageVolume(
            name=f'group/{group_name}',
            site=site,
            backend='zfs',
            host=host_doc.hostname,
            host_path=_standalone_host_path(
                site, host_doc, category='group', name=group_name,
                explicit=host_path, fallback=f'/group/{group_name}',
            ),
            storage_host=host_doc,
            allocations=[alloc],
            **_backend_config_kwargs('zfs'),
        )
    if parent_volume_name is None:
        raise ValueError(
            'Group storage requires a parent volume or host; pass '
            'parent_volume/--parent-volume or host/--host'
        )
    parent = await _find_volume(site, parent_volume_name)
    if parent is None:
        raise ValueError(
            f'Parent volume {parent_volume_name} does not exist on {site.name}'
        )
    return StorageVolume(
        name=f'{parent.name}/{group_name}',
        site=site,
        backend=parent.backend,
        host=parent.host,
        host_path=_join_host_path(parent.host_path, group_name),
        storage_host=await _inherit_storage_host(parent),
        parent=parent,
        allocations=[alloc],
        **_backend_config_kwargs(parent.backend),
    )


async def _resolve_group_mount(
    site: Site, *,
    automount_map: str | None, static_mount: str | None, no_mount: bool,
) -> tuple[AutomountMap | None, StaticMount | None]:
    """Resolve the mount mechanism for a group storage: explicit
    automount_map/static_mount by name, else the legacy fallback of an
    AutomountMap named 'group' on the site (mirrors home's 'home' fallback).
    Returns (amap, smount); (None, None) if no_mount or nothing resolves."""
    if no_mount:
        return None, None
    if automount_map is not None:
        amap = await AutomountMap.find_one(
            AutomountMap.name == automount_map,
            AutomountMap.site.id == site.id,
        )
        if amap is None:
            raise ValueError(
                f'AutomountMap {automount_map} does not exist on {site.name}'
            )
        return amap, None
    if static_mount is not None:
        smount = await StaticMount.find_one(
            StaticMount.name == static_mount,
            StaticMount.site.id == site.id,
        )
        if smount is None:
            raise ValueError(
                f'StaticMount {static_mount} does not exist on {site.name}'
            )
        return None, smount
    # Legacy fallback: a map conventionally named 'group'.
    amap = await AutomountMap.find_one(
        AutomountMap.name == 'group',
        AutomountMap.site.id == site.id,
    )
    return amap, None


async def _provision_group_storage(
    session: AsyncClientSession, *,
    site: Site, group: Group, owner: User,
    quota: str, parent_volume: str | None,
    automount_map: str | None, static_mount: str | None,
    no_mount: bool, host: str | None, host_path: str | None,
    comment: str | None = None,
) -> tuple[Storage, str]:
    """Build + insert the group volume and the group-facing Storage record,
    returning (storage, mechanism_label). The volume insert is pre-checked so a
    name clash surfaces as a clean ValueError rather than a DuplicateKeyError."""
    volume = await _build_group_volume(
        site, group.name, quota=quota, parent_volume_name=parent_volume,
        host=host, host_path=host_path, comment=comment,
    )
    if await _find_volume(site, volume.name) is not None:
        raise ValueError(
            f'StorageVolume {volume.name} already exists on {site.name}'
        )
    await volume.insert(session=session)

    amap, smount = await _resolve_group_mount(
        site, automount_map=automount_map, static_mount=static_mount,
        no_mount=no_mount,
    )
    mechanism = 'none'
    if amap is not None:
        mechanism = f'automount:{amap.name}'
    elif smount is not None:
        mechanism = f'static:{smount.mount_path}'

    storage = Storage(
        name=group.name,
        site=site,
        category='group',
        owner=owner,
        group=group,
        volume=volume,
        subpath='',
        automount_map=amap,
        mount_name=group.name if amap is not None else '',
        static_mount=smount,
    )
    await storage.insert(session=session)
    await ensure_group_site(group, site, session)
    return storage, mechanism


class CreateGroupStorage(Operation):
    """Provision a group's shared storage: a StorageVolume (a child under an
    explicit parent volume, or standalone on a host) plus the group-facing
    Storage record (category='group', mounted under /group via the site's
    'group' automount map by default).

    `Storage.owner` is required; it defaults to the group's sponsor at the site
    (the sponsor-role GroupMembership). If the group has no sponsor there, or
    more than one, an explicit `owner_name` is required.

    Unlike home storage there are no site-level group defaults, so a parent
    volume (or host) and a quota must be given. `comment` labels the initial
    quota allocation (default 'initial group allocation'), e.g. a ticket or
    purchase reference.
    """

    op_name = 'create_group_storage'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        group_name: str,
        site_name: str,
        owner_name: str | None = None,
        quota: str | None = None,
        comment: str | None = None,
        parent_volume: str | None = None,
        automount_map: str | None = None,
        static_mount: str | None = None,
        no_mount: bool = False,
        host: str | None = None,
        host_path: str | None = None,
    ) -> None:
        super().__init__(client, author)
        if automount_map is not None and static_mount is not None:
            raise ValueError(
                'automount_map and static_mount are mutually exclusive'
            )
        self.group_name = group_name
        self.site_name = site_name
        self.owner_name = owner_name
        self.quota = quota
        self.comment = comment
        self.parent_volume = parent_volume
        self.automount_map = automount_map
        self.static_mount = static_mount
        self.no_mount = no_mount
        self.host = host
        self.host_path = host_path
        # Resolved owner name, filled in by execute() for describe().
        self._owner_name = owner_name or ''
        self._mechanism = 'none'

    async def _resolve_owner(self, group: Group, site: Site) -> User:
        if self.owner_name is not None:
            owner = await User.find_one(User.name == self.owner_name)
            if owner is None:
                raise ValueError(f'User {self.owner_name} does not exist')
            return owner
        edges = await GroupMembership.find(
            GroupMembership.group.id == group.id,
            GroupMembership.site.id == site.id,
            fetch_links=True,
        ).to_list()
        sponsors = [e.user for e in edges if 'sponsor' in e.roles]
        if len(sponsors) == 1:
            return sponsors[0]
        if not sponsors:
            raise ValueError(
                f'Group {group.name} has no sponsor on {site.name}; '
                f'pass an explicit owner (--owner)'
            )
        names = ', '.join(sorted(s.name for s in sponsors))
        raise ValueError(
            f'Group {group.name} has multiple sponsors on {site.name} '
            f'({names}); pass an explicit owner (--owner)'
        )

    async def execute(self, session: AsyncClientSession) -> Storage:
        if self.quota is None:
            raise ValueError('Group storage requires a quota')
        if self.parent_volume is None and self.host is None:
            raise ValueError(
                'Group storage requires a parent volume or host; pass '
                'parent_volume/--parent-volume or host/--host'
            )

        group = await Group.find_one(
            Group.name == self.group_name, with_children=True,
        )
        if group is None:
            raise ValueError(f'Group {self.group_name} does not exist')

        site = await _find_site(self.site_name)
        owner = await self._resolve_owner(group, site)
        self._owner_name = owner.name

        existing = await Storage.find_one(
            Storage.name == self.group_name,
            Storage.site.id == site.id,
            Storage.category == 'group',
        )
        if existing is not None:
            raise ValueError(
                f'Group storage for {self.group_name} on {self.site_name} '
                f'already exists'
            )

        storage, mechanism = await _provision_group_storage(
            session, site=site, group=group, owner=owner,
            quota=self.quota, parent_volume=self.parent_volume,
            automount_map=self.automount_map, static_mount=self.static_mount,
            no_mount=self.no_mount, host=self.host, host_path=self.host_path,
            comment=self.comment,
        )
        self._mechanism = mechanism
        self._storage = storage
        return storage

    def describe(self) -> dict[str, Any]:
        return {
            'group': self.group_name,
            'site': self.site_name,
            'owner': self._owner_name or None,
            'quota': self.quota,
            'comment': self.comment,
            'parent_volume': self.parent_volume,
            'host': self.host,
            'mechanism': self._mechanism,
        }


class RehomeUser(Operation):
    """Move a user's home storage onto the site default home volume: remove
    the current home Storage record and recreate it from the site defaults
    (the work CreateHomeStorage does with no overrides).

    Only the user-facing Storage record is deleted; the old backing volume
    record is left in place (it may be a shared/subpath volume or hold a
    provisioned dataset). This changes cheeto's provisioning target only — it
    does NOT move files on disk. Transactional: if the recreate fails, the
    delete rolls back and the user keeps their current home.
    """

    op_name = 'rehome_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        user_name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.user_name = user_name
        self.site_name = site_name
        self._old_volume = ''
        self._new_volume = ''
        self._mechanism = 'none'

    async def execute(self, session: AsyncClientSession) -> Storage:
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        group = await Group.find_one(Group.name == self.user_name)
        if group is None:
            raise ValueError(f'Group {self.user_name} does not exist')

        site = await _find_site(self.site_name)

        current = await get_storage(site, self.user_name, 'home')
        if current is None:
            raise ValueError(
                f'No home storage for {self.user_name} on {self.site_name}'
            )

        default_id = link_target_id(site.storage.default_home_volume)
        if default_id is None:
            raise ValueError(
                f'Site {self.site_name} has no default home volume; set one '
                f'with `ng site set storage-defaults`'
            )
        if link_target_id(current.volume.parent) == default_id:
            raise ValueError(
                f'{self.user_name} home on {self.site_name} is already on the '
                f'site default home volume'
            )

        self._old_volume = current.volume.name
        await current.delete(session=session)

        storage, mechanism = await _provision_home_storage(
            session, site=site, user=user, group=group,
        )
        self._new_volume = storage.volume.name
        self._mechanism = mechanism
        self._storage = storage
        return storage

    def describe(self) -> dict[str, Any]:
        return {
            'user': self.user_name,
            'site': self.site_name,
            'old_volume': self._old_volume,
            'new_volume': self._new_volume,
            'mechanism': self._mechanism,
        }


# ---------------------------------------------------------------------------
# Automount maps + mount-mechanism management
# ---------------------------------------------------------------------------


class CreateAutomountMap(Operation):
    """Create an AutomountMap — an autofs table (e.g. 'home', 'group') that
    Storages attach to as their automount mechanism. Mirrors the record
    MigrateAutomountMaps builds from v1."""

    op_name = 'create_automount_map'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        name: str,
        prefix: str,
        options: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.name = name
        self.prefix = prefix
        self.options = list(options or [])

    async def execute(self, session: AsyncClientSession) -> AutomountMap:
        site = await _find_site(self.site_name)
        existing = await AutomountMap.find_one(
            AutomountMap.name == self.name,
            AutomountMap.site.id == site.id,
        )
        if existing is not None:
            raise ValueError(
                f'AutomountMap {self.name} already exists on {self.site_name}'
            )
        amap = AutomountMap(
            name=self.name, site=site, prefix=self.prefix,
            options=self.options,
        )
        await amap.insert(session=session)
        self._map = amap
        return amap

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'name': self.name,
            'prefix': self.prefix,
            'options': self.options,
        }


def _validate_mount_target(
    automount_map: str | None, static_mount: str | None, no_mount: bool,
) -> None:
    if sum((automount_map is not None, static_mount is not None, no_mount)) != 1:
        raise ValueError(
            'specify exactly one of automount_map, static_mount, or no_mount'
        )


async def _resolve_mount_target(
    site: Site, automount_map: str | None, static_mount: str | None,
) -> tuple[AutomountMap | None, StaticMount | None]:
    """Resolve a named automount map or static mount. The static mount is
    fetched with its volume (nesting_depth=1) so mount_path coverage checks
    work."""
    if automount_map is not None:
        amap = await AutomountMap.find_one(
            AutomountMap.name == automount_map,
            AutomountMap.site.id == site.id,
        )
        if amap is None:
            raise ValueError(
                f'AutomountMap {automount_map} does not exist on {site.name}'
            )
        return amap, None
    if static_mount is not None:
        smount = await StaticMount.find_one(
            StaticMount.name == static_mount,
            StaticMount.site.id == site.id,
            fetch_links=True, nesting_depth=1,
        )
        if smount is None:
            raise ValueError(
                f'StaticMount {static_mount} does not exist on {site.name}'
            )
        return None, smount
    return None, None


def _mount_label(
    amap: AutomountMap | None, smount: StaticMount | None, no_mount: bool,
) -> str:
    if amap is not None:
        return f'automount:{amap.name}'
    if smount is not None:
        return f'static:{smount.name}'
    return 'none'


def _apply_storage_mount(
    storage: Storage,
    *,
    amap: AutomountMap | None,
    smount: StaticMount | None,
    mount_name: str,
    no_mount: bool,
) -> None:
    """Set exactly one mount mechanism on `storage` in place, clearing the
    other so the at-most-one validator stays satisfied. `no_mount` (or neither
    target) clears both. Automount preserves any existing mount_overrides;
    switching to static/none clears them (they're automount-only)."""
    if no_mount or (amap is None and smount is None):
        storage.automount_map = None
        storage.mount_name = ''
        storage.mount_overrides = MountOverrides()
        storage.static_mount = None
        return
    if amap is not None:
        storage.static_mount = None
        storage.automount_map = amap
        storage.mount_name = mount_name or ''
        return
    storage.automount_map = None
    storage.mount_name = ''
    storage.mount_overrides = MountOverrides()
    storage.static_mount = smount


class SetStorageMount(Operation):
    """Set, change, or clear the mount mechanism on a single existing
    Storage — so mounting can be adjusted after migration/creation."""

    op_name = 'set_storage_mount'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        name: str,
        category: str | None = None,
        automount_map: str | None = None,
        mount_name: str = '',
        static_mount: str | None = None,
        no_mount: bool = False,
    ) -> None:
        super().__init__(client, author)
        _validate_mount_target(automount_map, static_mount, no_mount)
        self.site_name = site_name
        self.name = name
        self.category = category
        self.automount_map = automount_map
        self.mount_name = mount_name
        self.static_mount = static_mount
        self.no_mount = no_mount
        self._mechanism = 'none'

    async def execute(self, session: AsyncClientSession) -> Storage:
        site = await _find_site(self.site_name)
        storage = await get_storage(site, self.name, self.category)
        if storage is None:
            suffix = f' (category={self.category})' if self.category else ''
            raise ValueError(
                f'Storage {self.name} does not exist on {self.site_name}{suffix}'
            )
        amap, smount = await _resolve_mount_target(
            site, self.automount_map, self.static_mount,
        )
        _apply_storage_mount(
            storage, amap=amap, smount=smount,
            mount_name=self.mount_name, no_mount=self.no_mount,
        )
        self._mechanism = _mount_label(amap, smount, self.no_mount)
        await storage.save(session=session)
        self._storage = storage
        return storage

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'name': self.name,
            'category': self.category,
            'mechanism': self._mechanism,
        }


class SetVolumeStorageMounts(Operation):
    """Apply a mount-mechanism change to every Storage backed by a volume's
    full descendant subtree (the named volume plus all volumes nested under
    it). Switches a whole tree — e.g. all per-user homes under a `home`
    parent volume — between automount / static / no mount."""

    op_name = 'set_volume_storage_mounts'

    # Bulk: a home subtree can be thousands of storages, which would exceed the
    # server's transactionLifetimeLimitSeconds in one transaction. Run in a
    # bare session (per-storage saves commit individually); the op is
    # idempotent. Same rationale as _BulkMigrateOperation.
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        volume_name: str,
        automount_map: str | None = None,
        static_mount: str | None = None,
        no_mount: bool = False,
    ) -> None:
        super().__init__(client, author)
        _validate_mount_target(automount_map, static_mount, no_mount)
        self.site_name = site_name
        self.volume_name = volume_name
        self.automount_map = automount_map
        self.static_mount = static_mount
        self.no_mount = no_mount
        self._mechanism = 'none'
        self._updated = 0
        self._warnings: list[str] = []

    async def _descendant_volume_ids(self, site: Site, root: StorageVolume):
        """BFS the site's volume tree (one query, parent resolved) collecting
        the root volume id + all descendant volume ids."""
        volumes = await list_site_volumes(site)
        children: dict[Any, list[StorageVolume]] = {}
        for v in volumes:
            parent_id = link_target_id(v.parent)
            if parent_id is not None:
                children.setdefault(parent_id, []).append(v)
        ids = {root.id}
        frontier = [root]
        while frontier:
            nxt: list[StorageVolume] = []
            for v in frontier:
                for child in children.get(v.id, []):
                    if child.id not in ids:
                        ids.add(child.id)
                        nxt.append(child)
            frontier = nxt
        return ids

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        site = await _find_site(self.site_name)
        root = await _find_volume(site, self.volume_name)
        if root is None:
            raise ValueError(
                f'Volume {self.volume_name} does not exist on {self.site_name}'
            )
        amap, smount = await _resolve_mount_target(
            site, self.automount_map, self.static_mount,
        )
        self._mechanism = _mount_label(amap, smount, self.no_mount)

        subtree_ids = await self._descendant_volume_ids(site, root)
        storages = await Storage.find(
            In(Storage.volume.id, list(subtree_ids)),
            Storage.site.id == site.id,
            fetch_links=True,
            nesting_depth=2,
        ).to_list()

        for storage in storages:
            _apply_storage_mount(
                storage, amap=amap, smount=smount,
                mount_name='', no_mount=self.no_mount,
            )
            if smount is not None:
                # Surface storages the static mount can't cover (mount_path
                # would raise) without aborting the batch.
                try:
                    _ = storage.mount_path
                except ValueError as e:
                    self._warnings.append(f'{storage.name}: {e}')
            await storage.save(session=session)
            self._updated += 1

        return {
            'mechanism': self._mechanism if storages else None,
            'updated': self._updated,
            'warnings': list(self._warnings),
        }

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'volume': self.volume_name,
            'mechanism': self._mechanism,
            'storages_updated': self._updated,
            'warnings': list(self._warnings),
        }


# ---------------------------------------------------------------------------
# Puppet storage export
# ---------------------------------------------------------------------------


_PUPPET_BUCKETS = {
    # category -> (output key, zfs dataset permissions)
    'home': ('user', '0770'),
    'group': ('group', '2770'),
    'share': ('share', '2775'),
}


class ExportPuppetStorage(Operation):
    """Render a site's storage in the legacy puppet structure (v1's
    `_storage_to_puppet`): `{zfs|nfs: {user|group|share: {host: [entry]}}}`,
    with the `home` category mapped to the `user` key.

    Classification: a storage backed by a whole managed ZFS dataset
    (`subpath == ''` and the volume carries a `ZFSConfig`) is a `zfs` entry
    — puppet provisions the dataset (quota, permissions) plus its export.
    Anything else NFS-visible — subdirectory exports (Farm legacy homes)
    and unmanaged export roots (v1 plain-NFS bare volumes, `zfs=None`) —
    is an `nfs` entry: an exports line only. QuoByte-backed volumes don't
    participate (no NFS export; quotas live in QuoByte).

    Export options/ranges resolve per field through `effective_nfs_export`
    (storage -> volume -> StorageHost -> Site.storage), with the host and
    site tiers taken from the VOLUME's site — a Storage may live on another
    site than its volume. Hosts are looked up by `(site, hostname)` in one
    batched query, so no extra link depth is needed.

    Read-only; recorded in History.
    """

    op_name = 'export_puppet_storage'
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        sitename: str,
    ) -> None:
        super().__init__(client, author)
        self.sitename = sitename
        self._zfs_count = 0
        self._nfs_count = 0
        self._skipped = 0

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        site = await Site.find_one(Site.name == self.sitename)
        if site is None:
            raise ValueError(f'Site {self.sitename!r} does not exist')

        # Depth 1 resolves volume/owner/group — everything an entry needs.
        storages = await Storage.find(
            Storage.site.id == site.id,
            fetch_links=True,
            nesting_depth=1,
        ).sort('+name').to_list()

        # Default tiers, keyed by the VOLUME's site (cross-site storages).
        volume_site_ids = {
            sid for s in storages
            if (sid := link_target_id(s.volume.site)) is not None
        }
        sites_by_id = {site.id: site}
        other_ids = list(volume_site_ids - {site.id})
        if other_ids:
            for other in await Site.find(In(Site.id, other_ids)).to_list():
                sites_by_id[other.id] = other
        hosts = await storage_hosts_by_site(volume_site_ids)

        zfs: dict[str, dict] = {key: {} for key, _ in _PUPPET_BUCKETS.values()}
        nfs: dict[str, dict] = {key: {} for key, _ in _PUPPET_BUCKETS.values()}

        for storage in storages:
            key, perms = _PUPPET_BUCKETS[storage.category]
            volume = storage.volume
            if volume.backend != 'zfs':
                self._skipped += 1
                continue
            vsite_id = link_target_id(volume.site)
            export, _levels = effective_nfs_export(
                volume=volume,
                host=hosts.get((vsite_id, volume.host)),
                site=sites_by_id.get(vsite_id),
                storage=storage,
            )
            entry = {
                'name': storage.name,
                'owner': storage.owner.name,
                'group': storage.group.name,
                'path': storage.host_path,
                'export_options': export.export_options if export else '',
                'export_ranges': (
                    list(export.export_ranges) if export else []
                ),
            }
            if not storage.subpath and volume.zfs is not None:
                entry['quota'] = volume.quota
                entry['permissions'] = perms
                zfs[key].setdefault(volume.host, []).append(entry)
                self._zfs_count += 1
            else:
                nfs[key].setdefault(volume.host, []).append(entry)
                self._nfs_count += 1

        def _sort_hosts(buckets: dict[str, dict]) -> dict[str, dict]:
            return {
                key: {host: hosts[host] for host in sorted(hosts)}
                for key, hosts in buckets.items()
            }

        return {'zfs': _sort_hosts(zfs), 'nfs': _sort_hosts(nfs)}

    def describe(self) -> dict[str, Any]:
        return {
            'sitename': self.sitename,
            'zfs': self._zfs_count,
            'nfs': self._nfs_count,
            'skipped': self._skipped,
        }


# ---------------------------------------------------------------------------
# Volume allocations (quota blocks)
# ---------------------------------------------------------------------------


async def _find_volume_or_raise(site: Site, name: str) -> StorageVolume:
    volume = await _find_volume(site, name)
    if volume is None:
        raise ValueError(f'Volume {name} does not exist on {site.name}')
    return volume


def _check_alloc_index(volume: StorageVolume, index: int) -> None:
    n = len(volume.allocations)
    if not 0 <= index < n:
        raise ValueError(
            f'Volume {volume.name} has {n} allocation(s); no index {index}'
        )


class AddVolumeAllocation(Operation):
    """Append a quota allocation to a storage volume. A volume's total quota
    is the sum of its allocations; ExportPuppetStorage reads it to provision
    the dataset quota."""

    op_name = 'add_volume_allocation'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        volume_name: str,
        quota: str,
        comment: str = '',
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.volume_name = volume_name
        self.quota = quota
        self.comment = comment
        self._total = 0

    async def execute(self, session: AsyncClientSession) -> StorageVolume:
        site = await _find_site(self.site_name)
        volume = await _find_volume_or_raise(site, self.volume_name)
        volume.allocations.append(_make_allocation(self.quota, self.comment))
        await volume.save(session=session)
        self._total = len(volume.allocations)
        self._volume = volume
        return volume

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'volume': self.volume_name,
            'quota': self.quota,
            'comment': self.comment,
            'allocations': self._total,
        }


class RemoveVolumeAllocation(Operation):
    """Remove a quota allocation from a storage volume by 0-based index.
    Removing the last allocation leaves the volume with no quota."""

    op_name = 'remove_volume_allocation'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        volume_name: str,
        index: int,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.volume_name = volume_name
        self.index = index
        self._removed_quota = ''
        self._removed_comment = ''
        self._total = 0

    async def execute(self, session: AsyncClientSession) -> StorageVolume:
        site = await _find_site(self.site_name)
        volume = await _find_volume_or_raise(site, self.volume_name)
        _check_alloc_index(volume, self.index)
        removed = volume.allocations.pop(self.index)
        self._removed_quota = removed.quota
        self._removed_comment = removed.comment
        await volume.save(session=session)
        self._total = len(volume.allocations)
        self._volume = volume
        return volume

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'volume': self.volume_name,
            'index': self.index,
            'removed_quota': self._removed_quota,
            'removed_comment': self._removed_comment,
            'allocations': self._total,
        }


class EditVolumeAllocation(Operation):
    """Edit the quota (and optionally the comment) of an existing allocation
    on a storage volume, identified by 0-based index."""

    op_name = 'edit_volume_allocation'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        volume_name: str,
        index: int,
        quota: str,
        comment: str | None = None,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.volume_name = volume_name
        self.index = index
        self.quota = quota
        self.comment = comment
        self._old_quota = ''

    async def execute(self, session: AsyncClientSession) -> StorageVolume:
        site = await _find_site(self.site_name)
        volume = await _find_volume_or_raise(site, self.volume_name)
        _check_alloc_index(volume, self.index)
        old = volume.allocations[self.index]
        self._old_quota = old.quota
        new_comment = self.comment if self.comment is not None else old.comment
        volume.allocations[self.index] = _make_allocation(
            self.quota, new_comment,
        )
        await volume.save(session=session)
        self._volume = volume
        return volume

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'volume': self.volume_name,
            'index': self.index,
            'old_quota': self._old_quota,
            'new_quota': self.quota,
            'comment': self.comment,
        }


# ---------------------------------------------------------------------------
# Volume edit
# ---------------------------------------------------------------------------


class EditStorageVolume(Operation):
    """Edit an existing StorageVolume in place. Every field kwarg defaults to
    `UNSET` (leave alone); `clear_nfs_export=True` nulls the volume's own
    export config so it inherits the host/site defaults again.

    Structural fields (`host`, `host_path`, `new_name`) are refused while the
    volume has children: child volumes denormalize `host`/`host_path` from
    their parent and are named `<parent.name>/<child>`, so changing the
    parent would leave them stale. `host` resolves strictly to a StorageHost
    at the site and sets both `volume.host` and `volume.storage_host`.
    `volume.save()` fires `mark_storages_ldap_dirty`, so a host/host_path
    change re-syncs every Storage the volume backs.
    """

    op_name = 'edit_storage_volume'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        volume_name: str,
        new_name: Any = UNSET,
        host: Any = UNSET,
        host_path: Any = UNSET,
        export_options: Any = UNSET,
        export_ranges: Any = UNSET,
        clear_nfs_export: bool = False,
        zfs_dataset_name: Any = UNSET,
    ) -> None:
        super().__init__(client, author)
        if clear_nfs_export and (
            export_options is not UNSET or export_ranges is not UNSET
        ):
            raise ValueError(
                'clear_nfs_export is exclusive with export_options / '
                'export_ranges'
            )
        fields = (new_name, host, host_path, export_options, export_ranges,
                  zfs_dataset_name)
        if all(f is UNSET for f in fields) and not clear_nfs_export:
            raise ValueError('Nothing to edit; pass at least one field')
        self.site_name = site_name
        self.volume_name = volume_name
        self.new_name = new_name
        self.host = host
        self.host_path = host_path
        self.export_options = export_options
        self.export_ranges = export_ranges
        self.clear_nfs_export = clear_nfs_export
        self.zfs_dataset_name = zfs_dataset_name
        self._changes: dict[str, Any] = {}

    async def execute(self, session: AsyncClientSession) -> StorageVolume:
        site = await _find_site(self.site_name)
        volume = await _find_volume_or_raise(site, self.volume_name)
        changes: dict[str, Any] = {}

        structural = [
            label for label, value in (
                ('host', self.host), ('host_path', self.host_path),
                ('name', self.new_name),
            ) if value is not UNSET
        ]
        if structural:
            n_children = await StorageVolume.find(
                StorageVolume.parent.id == volume.id,
            ).count()
            if n_children:
                raise ValueError(
                    f'Volume {volume.name} has {n_children} child volume(s) '
                    f'whose host/host_path/name derive from it; refusing to '
                    f'change {", ".join(structural)}. Edit the children '
                    f'first or create a new volume.'
                )

        if self.host is not UNSET:
            host_doc = await _resolve_storage_host(site, self.host)
            if host_doc.hostname != volume.host:
                changes['old_host'] = volume.host
                changes['host'] = host_doc.hostname
            volume.host = host_doc.hostname
            volume.storage_host = host_doc

        if self.host_path is not UNSET:
            if not self.host_path:
                raise ValueError('host_path must not be empty')
            new_path = str(PurePosixPath(self.host_path))
            if new_path != volume.host_path:
                changes['old_host_path'] = volume.host_path
                changes['host_path'] = new_path
            volume.host_path = new_path

        if self.host is not UNSET or self.host_path is not UNSET:
            clash = await StorageVolume.find_one(
                StorageVolume.site.id == site.id,
                StorageVolume.host == volume.host,
                StorageVolume.host_path == volume.host_path,
                StorageVolume.id != volume.id,
            )
            if clash is not None:
                raise ValueError(
                    f'Volume {clash.name} already occupies '
                    f'{volume.host}:{volume.host_path} on {site.name}'
                )

        if self.new_name is not UNSET:
            if not self.new_name:
                raise ValueError('new_name must not be empty')
            if self.new_name != volume.name:
                if await _find_volume(site, self.new_name) is not None:
                    raise ValueError(
                        f'StorageVolume {self.new_name} already exists on '
                        f'{site.name}'
                    )
                changes['old_name'] = volume.name
                changes['name'] = self.new_name
                volume.name = self.new_name

        changes.update(apply_nfs_export(
            volume, export_options=self.export_options,
            export_ranges=self.export_ranges, clear=self.clear_nfs_export,
        ))

        if self.zfs_dataset_name is not UNSET:
            if volume.zfs is None:
                raise ValueError(
                    f'Volume {volume.name} is not a managed ZFS dataset '
                    f'(backend={volume.backend}, zfs config absent); cannot '
                    f'set a zfs dataset name'
                )
            volume.zfs.dataset_name = self.zfs_dataset_name or ''
            changes['zfs_dataset_name'] = volume.zfs.dataset_name

        try:
            await volume.save(session=session)
        except DuplicateKeyError as e:
            raise ValueError(
                f'Volume edit collides with an existing volume on '
                f'{site.name}: {e.details.get("errmsg", e) if e.details else e}'
            ) from e
        self._changes = changes
        self._volume = volume
        return volume

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'volume': self.volume_name,
            **self._changes,
        }
