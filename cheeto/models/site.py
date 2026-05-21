from __future__ import annotations

from typing import TYPE_CHECKING

import pymongo
from beanie import Insert, Link, Replace, Update, before_event
from pydantic import BaseModel, Field, model_validator
from pymongo import IndexModel

from .base import BaseDocument, link_target_id

if TYPE_CHECKING:
    from .group import Group
    from .slurm import SlurmAccount


class SiteSlurmSettings(BaseModel):
    """Per-site Slurm defaults.

    `sticky` is the set of SlurmAccounts every user at the site implicitly
    has access to (the v2 of v1's `Site.global_slurmers`). `default_account`
    is the user's default account at the site and, when set, must be one of
    the entries in `sticky`.
    """

    sticky: list[Link['SlurmAccount']] = Field(default_factory=list)
    default_account: Link['SlurmAccount'] | None = None

    @model_validator(mode='after')
    def _default_must_be_sticky(self) -> 'SiteSlurmSettings':
        if self.default_account is None:
            return self
        sticky_ids = {link_target_id(s) for s in self.sticky}
        if link_target_id(self.default_account) not in sticky_ids:
            raise ValueError(
                'SiteSlurmSettings.default_account must appear in '
                'SiteSlurmSettings.sticky'
            )
        return self


class SiteGroupSettings(BaseModel):
    """Per-site Group defaults.

    `sticky` is the set of Groups every user at the site is implicitly a
    member of (the v2 of v1's `Site.global_groups`).
    """

    sticky: list[Link['Group']] = Field(default_factory=list)


class Site(BaseDocument):
    name: str
    fqdn: str
    slurm: SiteSlurmSettings = Field(default_factory=SiteSlurmSettings)
    group: SiteGroupSettings = Field(default_factory=SiteGroupSettings)

    @before_event(Insert, Replace, Update)
    def _validate_sticky_settings(self) -> None:
        # Reconstruct the embedded models so pydantic re-runs their
        # validators. Closes the in-place-mutation hole (operator does
        # `site.slurm.sticky.clear()` then save, bypassing the validator
        # that fires on construction).
        SiteSlurmSettings.model_validate(self.slurm.model_dump())
        SiteGroupSettings.model_validate(self.group.model_dump())

    class Settings:
        name = 'sites'
        indexes = [
            IndexModel([('name', pymongo.ASCENDING)], unique=True),
        ]
