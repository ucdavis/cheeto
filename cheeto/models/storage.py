from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import pymongo
from beanie import Link
from pymongo import IndexModel
from pydantic import BaseModel, Field, field_validator

from ..constants import DATA_QUOTA_REGEX, STORAGE_CATEGORIES, STORAGE_TYPES
from ..utils import size_to_megs
from .base import BaseDocument, Expirable
from .site import Site
from .user import User

if TYPE_CHECKING:
    from .group import Group


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


class AutomountMap(BaseDocument):
    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]
    prefix: Annotated[str, Field(min_length=1)]
    options: list[str] = Field(default_factory=list)

    class Settings:
        name = 'automount_maps'
        indexes = [
            IndexModel(
                [('name', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]


class Storage(BaseDocument, Expirable):
    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]
    type: str
    category: str

    owner: Link[User]
    group: Link['Group']

    host: Annotated[str, Field(min_length=1)]
    host_path: str = ''

    allocations: list[StorageAllocation] = Field(default_factory=list)

    nfs_export: NFSExportConfig | None = None

    automount_map: Link[AutomountMap] | None = None
    mount_name: str = ''
    mount_overrides: MountOverrides = Field(default_factory=MountOverrides)

    globus: bool = False

    @field_validator('type')
    @classmethod
    def validate_type(cls, v):
        if v not in STORAGE_TYPES:
            raise ValueError(f'Invalid storage type: {v}')
        return v

    @field_validator('category')
    @classmethod
    def validate_category(cls, v):
        if v not in STORAGE_CATEGORIES:
            raise ValueError(f'Invalid storage category: {v}')
        return v

    @property
    def quota(self) -> str | None:
        if not self.allocations:
            return None
        total_megs = sum(size_to_megs(a.quota) for a in self.allocations)
        if total_megs >= 1024 * 1024:
            return f'{total_megs / (1024 * 1024):.10g}T'
        if total_megs >= 1024:
            return f'{total_megs / 1024:.10g}G'
        return f'{total_megs}M'

    @property
    def mount_options(self) -> list[str]:
        if self.mount_overrides.options:
            return list(self.mount_overrides.options)
        if self.automount_map is None:
            return []
        base = set(self.automount_map.options)
        base -= set(self.mount_overrides.remove_options)
        base |= set(self.mount_overrides.add_options)
        return sorted(base)

    @property
    def mount_path(self) -> str:
        if self.automount_map is None:
            return ''
        return str(Path(self.automount_map.prefix) / self.mount_name)

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
            [('type', pymongo.ASCENDING)],
        ]
