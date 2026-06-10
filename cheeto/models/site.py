from __future__ import annotations

from typing import Annotated

import pymongo
from beanie import Insert, Replace, Save, SaveChanges, Update, before_event
from pydantic import BaseModel, Field, model_validator
from pymongo import IndexModel

from ..constants import DATA_QUOTA_REGEX
from .base import BaseDocument, DocRef


class SiteSlurmSettings(BaseModel):
    """Per-site Slurm defaults.

    `sticky` is the set of SlurmAccounts every user at the site implicitly
    has access to (the v2 of v1's `Site.global_slurmers`). `default_account`
    is the user's default account at the site and, when set, must be one of
    the entries in `sticky`.

    References are stored as bare ObjectIds (`DocRef`), NOT `Link`s — links
    inside embedded models are invisible to beanie and silently degrade to
    inline snapshots (see `models/base.py::DocRef`).
    """

    sticky: list[DocRef] = Field(default_factory=list)   # -> SlurmAccount
    default_account: DocRef | None = None                # -> SlurmAccount

    @model_validator(mode='after')
    def _default_must_be_sticky(self) -> 'SiteSlurmSettings':
        if self.default_account is None:
            return self
        if self.default_account not in set(self.sticky):
            raise ValueError(
                'SiteSlurmSettings.default_account must appear in '
                'SiteSlurmSettings.sticky'
            )
        return self


class SiteGroupSettings(BaseModel):
    """Per-site Group defaults.

    `sticky` is the set of Groups every user at the site is implicitly a
    member of (the v2 of v1's `Site.global_groups`). Stored as `DocRef`s —
    see `SiteSlurmSettings`.
    """

    sticky: list[DocRef] = Field(default_factory=list)   # -> Group


class SiteStorageSettings(BaseModel):
    """Per-site storage defaults used by CreateHomeStorage.

    `default_home_volume` is the PARENT volume new per-user home datasets
    are provisioned under; `default_home_quota` is the quota applied to
    each. Exactly one (or neither) of `home_automount_map` /
    `home_static_mount` selects the mount mechanism for new homes (Farm:
    automount; Hive: static). Stored as `DocRef`s — see
    `SiteSlurmSettings`."""

    default_home_volume: DocRef | None = None            # -> StorageVolume
    default_home_quota: Annotated[
        str, Field(pattern=DATA_QUOTA_REGEX)
    ] | None = None
    home_automount_map: DocRef | None = None             # -> AutomountMap
    home_static_mount: DocRef | None = None              # -> StaticMount

    @model_validator(mode='after')
    def _one_mount_mechanism(self) -> 'SiteStorageSettings':
        if (
            self.home_automount_map is not None
            and self.home_static_mount is not None
        ):
            raise ValueError(
                'SiteStorageSettings: home_automount_map and '
                'home_static_mount are mutually exclusive'
            )
        return self


def _renormalize(settings: BaseModel) -> BaseModel:
    """Re-validate an embedded settings model from its LIVE attribute
    values, returning the normalized copy. Coerces anything appended/
    assigned in place (full documents, Links, snapshot dicts) to bare ids
    via the DocRef BeforeValidator, and re-runs the model validators —
    without a model_dump() round trip (which would emit serializer warnings
    for non-id values)."""
    cls = type(settings)
    return cls.model_validate(
        {name: getattr(settings, name) for name in cls.model_fields}
    )


class Site(BaseDocument):
    name: str
    fqdn: str
    slurm: SiteSlurmSettings = Field(default_factory=SiteSlurmSettings)
    group: SiteGroupSettings = Field(default_factory=SiteGroupSettings)
    storage: SiteStorageSettings = Field(default_factory=SiteStorageSettings)

    # Two beanie traps this hook's shape avoids (tripwire-tested):
    # - the name must NOT start with '_': beanie's init_actions silently
    #   skips underscore-prefixed attributes, leaving the hook unregistered;
    # - Save/SaveChanges must be subscribed in addition to Insert/Replace/
    #   Update: save() fires SAVE and pre-serializes the document into its
    #   update payload BEFORE the inner update()'s UPDATE actions run.
    @before_event(Insert, Replace, Save, SaveChanges, Update)
    def normalize_settings(self) -> None:
        # Normalize-and-reassign: re-runs the embedded validators (closing
        # the in-place-mutation hole — e.g. `site.slurm.sticky.clear()` then
        # save) AND coerces any document/Link appended in place back to a
        # bare id, so saves can never serialize an inline snapshot.
        self.slurm = _renormalize(self.slurm)
        self.group = _renormalize(self.group)
        self.storage = _renormalize(self.storage)

    class Settings:
        name = 'sites'
        indexes = [
            IndexModel([('name', pymongo.ASCENDING)], unique=True),
        ]
