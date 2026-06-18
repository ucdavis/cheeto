from __future__ import annotations

from typing import Any

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.base import link_target_id
from ..models.group import Group
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
from ..models.user import User
from ..queries.storage import get_storage, list_site_volumes
from .base import Operation


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


class CreateStorageVolume(Operation):
    """Create a StorageVolume record — the provisionable backing entity (a
    ZFS dataset or QuoByte volume). Does not (yet) provision anything on
    the backend itself."""

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
        host_path: str,
        parent_name: str | None = None,
        quota: str | None = None,
        export_options: str | None = None,
        export_ranges: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.name = name
        self.backend = backend
        self.host = host
        self.host_path = host_path
        self.parent_name = parent_name
        self.quota = quota
        self.export_options = export_options
        self.export_ranges = export_ranges

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

        nfs_export = None
        if self.export_options or self.export_ranges:
            nfs_export = NFSExportConfig(
                export_options=self.export_options or '',
                export_ranges=self.export_ranges or [],
            )

        volume = StorageVolume(
            name=self.name,
            site=site,
            backend=self.backend,
            host=self.host,
            host_path=self.host_path,
            parent=parent,
            allocations=(
                [StorageAllocation(quota=self.quota, comment='initial allocation')]
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
            'host_path': self.host_path,
            'parent': self.parent_name,
            'quota': self.quota,
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

    async def _resolve_parent(self, site: Site) -> StorageVolume:
        if self.parent_volume is not None:
            parent = await _find_volume(site, self.parent_volume)
            if parent is None:
                raise ValueError(
                    f'Parent volume {self.parent_volume} does not exist on '
                    f'{self.site_name}'
                )
            return parent
        default_id = link_target_id(site.storage.default_home_volume)
        if default_id is None:
            raise ValueError(
                f'Site {self.site_name} has no default home volume; pass '
                f'parent_volume/--parent-volume or host/--host, or set one '
                f'with `ng site storage set-defaults`'
            )
        parent = await StorageVolume.get(default_id)
        if parent is None:
            raise ValueError(
                f'Site {self.site_name} default home volume is dangling'
            )
        return parent

    async def _build_volume(self, site: Site) -> StorageVolume:
        if self.host is not None:
            return StorageVolume(
                name=f'home/{self.user_name}',
                site=site,
                backend='zfs',
                host=self.host,
                host_path=self.host_path or f'/home/{self.user_name}',
                allocations=(
                    [StorageAllocation(
                        quota=self.quota, comment='initial home allocation',
                    )] if self.quota else []
                ),
                **_backend_config_kwargs('zfs'),
            )

        parent = await self._resolve_parent(site)
        quota = self.quota or site.storage.default_home_quota
        return StorageVolume(
            name=f'{parent.name}/{self.user_name}',
            site=site,
            backend=parent.backend,
            host=parent.host,
            host_path=_join_host_path(parent.host_path, self.user_name),
            parent=parent,
            allocations=(
                [StorageAllocation(
                    quota=quota, comment='initial home allocation',
                )] if quota else []
            ),
            **_backend_config_kwargs(parent.backend),
        )

    async def _resolve_mount(
        self, site: Site,
    ) -> tuple[AutomountMap | None, StaticMount | None]:
        if self.no_mount:
            return None, None
        if self.automount_map is not None:
            amap = await AutomountMap.find_one(
                AutomountMap.name == self.automount_map,
                AutomountMap.site.id == site.id,
            )
            if amap is None:
                raise ValueError(
                    f'AutomountMap {self.automount_map} does not exist on '
                    f'{self.site_name}'
                )
            return amap, None
        if self.static_mount is not None:
            smount = await StaticMount.find_one(
                StaticMount.name == self.static_mount,
                StaticMount.site.id == site.id,
            )
            if smount is None:
                raise ValueError(
                    f'StaticMount {self.static_mount} does not exist on '
                    f'{self.site_name}'
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

        volume = await self._build_volume(site)
        await volume.insert(session=session)

        amap, smount = await self._resolve_mount(site)
        if amap is not None:
            self._mechanism = f'automount:{amap.name}'
        elif smount is not None:
            self._mechanism = f'static:{smount.mount_path}'

        storage = Storage(
            name=self.user_name,
            site=site,
            category='home',
            owner=user,
            group=group,
            volume=volume,
            subpath='',
            automount_map=amap,
            mount_name=self.user_name if amap is not None else '',
            static_mount=smount,
        )
        await storage.insert(session=session)
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

        zfs: dict[str, dict] = {key: {} for key, _ in _PUPPET_BUCKETS.values()}
        nfs: dict[str, dict] = {key: {} for key, _ in _PUPPET_BUCKETS.values()}

        for storage in storages:
            key, perms = _PUPPET_BUCKETS[storage.category]
            volume = storage.volume
            if volume.backend != 'zfs':
                self._skipped += 1
                continue
            export = storage.nfs_export or volume.nfs_export
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
