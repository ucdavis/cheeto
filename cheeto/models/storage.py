"""Storage models: volumes, mounts, and user-facing storage records.

The architecture separates three concerns:

- **StorageVolume** — the real provisionable backing entity on a storage
  host: a ZFS dataset (usually quota'd) or a QuoByte volume (today managed
  via its UI; the embedded `QuobyteConfig` is the seam for future API
  provisioning). Volumes can nest (`parent`) — e.g. per-user home datasets
  provisioned under a site's central home volume.
- **Mount mechanisms** — how a cluster sees a storage: `AutomountMap`
  (LDAP autofs tables, e.g. Farm) or `StaticMount` (fstab-style mounts at
  fixed roots, e.g. Hive).
- **Storage** — the user-facing record: a named, owned thing backed by
  `(volume, subpath)` and mounted by at most one mechanism. Plain exported
  subdirectories (e.g. Farm legacy homes carved out of group ZFS volumes)
  are a `Storage` with a non-empty `subpath`, never their own volume.

Invariant that keeps consumers at `nesting_depth=1`: `StorageVolume.host`
and `host_path` are always concrete — denormalized from the parent at
creation time, never resolved through the `parent` link.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal

import pymongo
from beanie import (
    Delete,
    Insert,
    Link,
    Replace,
    Save,
    SaveChanges,
    Update,
    after_event,
    before_event,
)
from pymongo import IndexModel
from pydantic import BaseModel, Field, field_validator, model_validator

from ..constants import DATA_QUOTA_REGEX, MOUNT_FSTYPES, STORAGE_CATEGORIES
from ..utils import megs_to_size, size_to_megs_exact
from .base import BaseDocument, Expirable, link_target_id
from .ldap_sync import (
    LDAPSyncable,
    ldap_touch,
    queue_ldap_touch,
    stable_fingerprint,
)
from .site import Site
from .user import User

if TYPE_CHECKING:
    from .group import Group


StorageBackend = Literal['zfs', 'quobyte']


def _require_fetched(link, what: str):
    """Derived properties read through links; fail loudly when the caller
    forgot fetch_links=True instead of misbehaving on a beanie Link proxy."""
    if isinstance(link, Link):
        raise RuntimeError(
            f'{what} is an unfetched Link; query with fetch_links=True and '
            f'sufficient nesting_depth (see queries/storage.py helpers)'
        )
    return link


def _join_host_path(base: str, sub: str) -> str:
    if not sub:
        return base
    return str(PurePosixPath(base) / sub)


def _format_quota(allocations: list['StorageAllocation']) -> str | None:
    if not allocations:
        return None
    # Exact decimal arithmetic: float/int round-tripping renders a 45.8T
    # allocation as 45.79999924T.
    total_megs = sum(
        (size_to_megs_exact(a.quota) for a in allocations), Decimal(0),
    )
    return megs_to_size(total_megs)


class StorageAllocation(BaseModel):
    quota: Annotated[str, Field(pattern=DATA_QUOTA_REGEX)]
    comment: str = ''


class NFSExportConfig(BaseModel):
    export_options: str = ''
    export_ranges: list[str] = Field(default_factory=list)


class MountOverrides(BaseModel):
    options: list[str] = Field(default_factory=list)
    add_options: list[str] = Field(default_factory=list)
    remove_options: list[str] = Field(default_factory=list)


class ZFSConfig(BaseModel):
    """Marker + future knobs for a real ZFS dataset. `dataset_name` is the
    zfs-side identifier (e.g. 'flash/export/home/maccamp'); '' means
    'derive from host_path'."""

    dataset_name: str = ''


class QuobyteConfig(BaseModel):
    """Placeholder for QuoByte API provisioning — volume creation and
    quota management land here in a later pass."""

    volume_id: str = ''
    tenant: str = ''


class AutomountMap(BaseDocument):
    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]
    prefix: Annotated[str, Field(min_length=1)]
    options: list[str] = Field(default_factory=list)

    @after_event(Insert, Replace, Update, Delete)
    async def mark_storages_ldap_dirty(self) -> None:
        # Storage.mount_options derives from this map's options; mark the
        # dependent rows dirty directly. Update covers save()/save_changes();
        # sessionless write — deferred past any active Operation
        # transaction (see ldap_sync docstring).
        map_id = self.id
        await queue_ldap_touch(
            lambda: Storage.find(
                Storage.automount_map.id == map_id,
            ).update_many(ldap_touch())
        )

    class Settings:
        name = 'automount_maps'
        indexes = [
            IndexModel(
                [('name', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]


class StorageVolume(BaseDocument, Expirable):
    """A real provisionable backing entity: a ZFS dataset or a QuoByte
    volume. NOT user-facing and NOT a mount — `Storage` records reference
    volumes. Plain exported subdirectories are `Storage.subpath`, never a
    volume.

    `host`/`host_path` are always concrete (denormalized from `parent` at
    creation time) so consumers never need to traverse `parent`; the link
    is hierarchy/provisioning metadata only. `provisioned_at` (Expirable)
    tracks when the volume actually exists on the backend.
    """

    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]
    backend: StorageBackend
    zfs: ZFSConfig | None = None
    quobyte: QuobyteConfig | None = None

    host: Annotated[str, Field(min_length=1)]
    host_path: Annotated[str, Field(min_length=1)]

    parent: Link['StorageVolume'] | None = None

    @field_validator('host_path')
    @classmethod
    def _normalize_host_path(cls, v):
        # v1 data carries trailing slashes (e.g. collection prefixes like
        # '/nas-4-1/home/'); normalize so path equality and derived
        # Storage.host_path strings are canonical.
        return str(PurePosixPath(v))

    allocations: list[StorageAllocation] = Field(default_factory=list)
    nfs_export: NFSExportConfig | None = None

    def _check_backend_config(self) -> None:
        if self.backend == 'zfs' and self.quobyte is not None:
            raise ValueError(
                'zfs-backed volume must not carry a QuobyteConfig'
            )
        if self.backend == 'quobyte' and self.zfs is not None:
            raise ValueError(
                'quobyte-backed volume must not carry a ZFSConfig'
            )

    @model_validator(mode='after')
    def _validate(self) -> 'StorageVolume':
        self._check_backend_config()
        return self

    # Public name + Save/SaveChanges subscription required — see the
    # normalize_settings comment in models/site.py (beanie silently skips
    # underscore-prefixed hooks, and save() encodes before UPDATE actions).
    @before_event(Insert, Replace, Save, SaveChanges, Update)
    def revalidate(self) -> None:
        # Close the in-place-mutation hole (the Site settings pattern).
        self._check_backend_config()

    @after_event(Insert, Replace, Update, Delete)
    async def mark_storages_ldap_dirty(self) -> None:
        # Storage.host/host_path derive from this volume; mark the dependent
        # rows dirty directly. Update covers save()/save_changes();
        # sessionless write — deferred past any active Operation
        # transaction (see ldap_sync docstring).
        volume_id = self.id
        await queue_ldap_touch(
            lambda: Storage.find(
                Storage.volume.id == volume_id,
            ).update_many(ldap_touch())
        )

    @property
    def quota(self) -> str | None:
        return _format_quota(self.allocations)

    class Settings:
        name = 'storage_volumes'
        indexes = [
            IndexModel(
                [('name', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [
                    ('site', pymongo.ASCENDING),
                    ('host', pymongo.ASCENDING),
                    ('host_path', pymongo.ASCENDING),
                ],
                unique=True,
            ),
            [('parent', pymongo.ASCENDING)],
            [('site', pymongo.ASCENDING), ('backend', pymongo.ASCENDING)],
        ]


class StaticMount(BaseDocument):
    """An fstab-style mount on a static-mount cluster (e.g. Hive). Either
    backed by a StorageVolume (+ optional subpath under it) or a raw `spec`
    for non-volume filesystems (cvmfs)."""

    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]
    fstype: str
    volume: Link[StorageVolume] | None = None
    subpath: str = ''
    spec: str = ''
    mount_path: Annotated[str, Field(min_length=1)]
    options: list[str] = Field(default_factory=list)

    @field_validator('fstype')
    @classmethod
    def validate_fstype(cls, v):
        if v not in MOUNT_FSTYPES:
            raise ValueError(f'Invalid mount fstype: {v}')
        return v

    @field_validator('subpath')
    @classmethod
    def _subpath_relative(cls, v):
        if v.startswith('/'):
            raise ValueError(
                'StaticMount.subpath must be relative to the volume root'
            )
        return v.rstrip('/')

    def _check_source(self) -> None:
        if self.volume is None and not self.spec:
            raise ValueError('StaticMount requires either volume or spec')
        if self.volume is not None and self.spec:
            raise ValueError(
                'StaticMount volume and spec are mutually exclusive'
            )
        if self.volume is None and self.subpath:
            raise ValueError('StaticMount.subpath requires volume')

    @model_validator(mode='after')
    def _validate(self) -> 'StaticMount':
        self._check_source()
        return self

    # Public name + Save/SaveChanges subscription required — see the
    # normalize_settings comment in models/site.py (beanie silently skips
    # underscore-prefixed hooks, and save() encodes before UPDATE actions).
    @before_event(Insert, Replace, Save, SaveChanges, Update)
    def revalidate(self) -> None:
        self._check_source()

    @property
    def host_path(self) -> str:
        """Absolute host-side path covered by this mount; '' for raw-spec
        mounts. Requires `volume` fetched."""
        if self.volume is None:
            return ''
        v = _require_fetched(self.volume, 'StaticMount.volume')
        return _join_host_path(v.host_path, self.subpath)

    @property
    def device_spec(self) -> str:
        """fstab column 1: 'host:/path' for volume mounts, raw spec otherwise."""
        if self.volume is None:
            return self.spec
        v = _require_fetched(self.volume, 'StaticMount.volume')
        return f'{v.host}:{_join_host_path(v.host_path, self.subpath)}'

    class Settings:
        name = 'static_mounts'
        indexes = [
            IndexModel(
                [('site', pymongo.ASCENDING), ('mount_path', pymongo.ASCENDING)],
                unique=True,
            ),
            IndexModel(
                [('name', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]


class Storage(LDAPSyncable, BaseDocument, Expirable):
    """User-facing record: a named thing, owned by (owner, group), backed
    by `(volume, subpath)`, visible via at most one mount mechanism —
    automount (LDAP autofs) or static (fstab). Both None is legal (e.g.
    QuoByte native-client storages).

    `storage.site` may differ from `volume.site`: v1's `mount_source_site`
    pattern (a Farm volume mounted on another cluster) is real data.
    """

    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]
    category: str

    owner: Link[User]
    group: Link['Group']

    volume: Link[StorageVolume]
    subpath: str = ''

    # A subdirectory export is a real /etc/exports line with its own
    # options/ranges (Farm legacy homes); None for most storages, whose
    # export config lives on the volume.
    nfs_export: NFSExportConfig | None = None

    # Mount mechanism A: LDAP automount (e.g. Farm).
    automount_map: Link[AutomountMap] | None = None
    mount_name: str = ''
    mount_overrides: MountOverrides = Field(default_factory=MountOverrides)

    # Mount mechanism B: fstab static mount (e.g. Hive).
    static_mount: Link[StaticMount] | None = None

    globus: bool = False

    @field_validator('category')
    @classmethod
    def validate_category(cls, v):
        if v not in STORAGE_CATEGORIES:
            raise ValueError(f'Invalid storage category: {v}')
        return v

    @field_validator('subpath')
    @classmethod
    def _subpath_relative(cls, v):
        if v.startswith('/'):
            raise ValueError(
                'Storage.subpath must be relative to the volume root'
            )
        return v.rstrip('/')

    def _check_mount_mechanism(self) -> None:
        if self.automount_map is not None and self.static_mount is not None:
            raise ValueError(
                'Storage may use automount_map (LDAP autofs) or static_mount '
                '(fstab), not both'
            )
        if self.static_mount is not None and self.mount_name:
            raise ValueError(
                'mount_name is only meaningful with automount_map'
            )

    @model_validator(mode='after')
    def _validate(self) -> 'Storage':
        self._check_mount_mechanism()
        return self

    # Public name + Save/SaveChanges subscription required — see the
    # normalize_settings comment in models/site.py (beanie silently skips
    # underscore-prefixed hooks, and save() encodes before UPDATE actions).
    @before_event(Insert, Replace, Save, SaveChanges, Update)
    def revalidate(self) -> None:
        self._check_mount_mechanism()

    def ldap_fingerprint(self) -> str:
        # Own fields only: host/host_path/mount_options are properties over
        # links that may be unfetched at save time (_require_fetched raises).
        # Volume- and map-derived changes arrive via the propagation hooks
        # on StorageVolume / AutomountMap instead.
        return stable_fingerprint({
            'name': self.name,
            'category': self.category,
            'subpath': self.subpath,
            'mount_name': self.mount_name,
            'mount_overrides': self.mount_overrides.model_dump(),
            'volume': str(link_target_id(self.volume)),
            'automount_map': str(link_target_id(self.automount_map)),
            'static_mount': str(link_target_id(self.static_mount)),
        })

    @property
    def host(self) -> str:
        return _require_fetched(self.volume, 'Storage.volume').host

    @property
    def host_path(self) -> str:
        v = _require_fetched(self.volume, 'Storage.volume')
        return _join_host_path(v.host_path, self.subpath)

    @property
    def quota(self) -> str | None:
        """Quota is a VOLUME property. Subpath storages (e.g. legacy Farm
        homes exported out of group volumes) have no quota of their own."""
        if self.subpath:
            return None
        return _require_fetched(self.volume, 'Storage.volume').quota

    @property
    def mount_options(self) -> list[str]:
        if self.static_mount is not None:
            sm = _require_fetched(self.static_mount, 'Storage.static_mount')
            return list(sm.options)
        if self.mount_overrides.options:
            return list(self.mount_overrides.options)
        if self.automount_map is None:
            return []
        m = _require_fetched(self.automount_map, 'Storage.automount_map')
        base = set(m.options)
        base -= set(self.mount_overrides.remove_options)
        base |= set(self.mount_overrides.add_options)
        return sorted(base)

    @property
    def mount_path(self) -> str:
        if self.automount_map is not None:
            m = _require_fetched(self.automount_map, 'Storage.automount_map')
            return str(PurePosixPath(m.prefix) / (self.mount_name or self.name))
        if self.static_mount is not None:
            # Needs nesting_depth=2: static_mount.volume must be fetched.
            sm = _require_fetched(self.static_mount, 'Storage.static_mount')
            mount_root = sm.host_path
            if not mount_root:
                raise ValueError(
                    f'Storage {self.name!r} references spec-only static '
                    f'mount {sm.name!r}; no path derivation possible'
                )
            mine = self.host_path
            if mine == mount_root:
                return sm.mount_path
            prefix = mount_root.rstrip('/') + '/'
            if not mine.startswith(prefix):
                raise ValueError(
                    f'Storage {self.name!r} host path {mine} is not under '
                    f'static mount {sm.name!r} ({mount_root})'
                )
            return str(PurePosixPath(sm.mount_path) / mine[len(prefix):])
        return ''

    class Settings:
        name = 'storages'
        indexes = [
            IndexModel(
                [
                    ('name', pymongo.ASCENDING),
                    ('site', pymongo.ASCENDING),
                    ('category', pymongo.ASCENDING),
                ],
                unique=True,
            ),
            [('owner', pymongo.ASCENDING)],
            [('group', pymongo.ASCENDING)],
            [('site', pymongo.ASCENDING), ('category', pymongo.ASCENDING)],
            [('volume', pymongo.ASCENDING)],
        ]
