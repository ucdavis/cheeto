"""Host documents: machines that belong to a site.

`Host` is the polymorphic root (`is_root=True`, collection `hosts`).
Subclasses share the collection and are distinguished by beanie's
`_class_id`, the same shape as `Group` / `AccessGroup` / `StatusGroup`.
`StorageHost` is the first subclass: a NAS or file server that carries the
per-host storage defaults (NFS export config, ZFS path templates) that
volumes on it inherit. `StorageVolume.storage_host` links to it while
`StorageVolume.host` keeps the concrete hostname string every consumer reads.

Query rule: `Host.find(...)` without `with_children=True` adds a
`_class_id == 'Host'` filter and HIDES every subclass row. Resolve storage
hosts through `StorageHost.find_one(...)` (auto-filtered to its own class),
and pass `with_children=True` when iterating the root (the site-delete
cascade does). A plain `Host` row fetched through a `Link[StorageHost]`
would parse as a `StorageHost` with default fields, so never create bare
`Host` rows for storage servers.

Identity is `(site, hostname)`: the same physical NAS serves more than one
site in live data (`nas-8-0` on farm and franklin), and per-site defaults
differ, so hosts are per-site records.
"""

from __future__ import annotations

from typing import Annotated

import pymongo
from beanie import Insert, Link, Replace, Save, SaveChanges, Update, before_event
from pydantic import Field, field_validator
from pymongo import IndexModel

from .base import BaseDocument
from .site import Site
from .storage_defaults import StorageDefaults, validate_zfs_path_templates


def validate_hostname(v: str) -> str:
    """Normalize + validate a hostname. Bare labels (`nas-4-1`, `flash`,
    `c8-94`) and IPs are all real; only reject shapes that would break
    `host:/path` device specs. Module-level so operations can call it before
    constructing a document and raise a plain ValueError."""
    v = v.strip()
    if not v:
        raise ValueError('hostname must not be empty')
    if any(c.isspace() for c in v) or '/' in v or ':' in v:
        raise ValueError(
            f'Invalid hostname {v!r}: whitespace, "/" and ":" are not allowed'
        )
    return v


class Host(BaseDocument):
    hostname: Annotated[str, Field(min_length=1)]
    site: Link[Site]

    @field_validator('hostname')
    @classmethod
    def _validate_hostname(cls, v: str) -> str:
        return validate_hostname(v)

    class Settings:
        name = 'hosts'
        is_root = True
        indexes = [
            IndexModel(
                [('site', pymongo.ASCENDING), ('hostname', pymongo.ASCENDING)],
                unique=True,
            ),
        ]


class StorageHost(Host, StorageDefaults):
    """A storage server at a site, carrying host-level defaults.

    `nfs_export` and `zfs_path_templates` (from `StorageDefaults`) sit
    between the volume tier and the site tier in the precedence chain. No
    nested `Settings`: the root's collection and indexes apply (the
    `AccessGroup` precedent).
    """

    # Public name + Save/SaveChanges subscription required — see the
    # normalize_settings comment in models/site.py. Closes the in-place
    # mutation hole for `host.zfs_path_templates['home'] = ...; save()`.
    @before_event(Insert, Replace, Save, SaveChanges, Update)
    def revalidate(self) -> None:
        validate_zfs_path_templates(self.zfs_path_templates)
