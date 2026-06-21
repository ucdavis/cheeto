from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

import pymongo
from beanie import Link
from pymongo import IndexModel
from pydantic import BaseModel, Field, field_validator

from ..constants import DATA_QUOTA_REGEX, SLURM_QOS_VALID_FLAGS
from ..utils import size_to_megs
from .base import BaseDocument, Expirable
from .site import Site
from .user import User

if TYPE_CHECKING:
    from .group import Group


class SlurmTRES(BaseModel):
    """Trackable resources for a Slurm allocation.

    `cpus` and `gpus` use None to mean 'unlimited' (translated to -1 when
    emitted to slurm/sacctmgr). The validator also accepts -1 on input and
    normalizes it to None for storage clarity.
    """

    cpus: int | None = None
    gpus: int | None = None
    mem: Annotated[str | None, Field(default=None, pattern=DATA_QUOTA_REGEX)]

    @field_validator('cpus', 'gpus', mode='before')
    @classmethod
    def _normalize_unlimited(cls, v):
        if v == -1 or v == '-1':
            return None
        return v

    @property
    def slurm_cpus(self) -> int:
        return -1 if self.cpus is None else self.cpus

    @property
    def slurm_gpus(self) -> int:
        return -1 if self.gpus is None else self.gpus

    def to_slurm(self) -> str:
        """Render as a sacctmgr-compatible TRES string. Unlimited fields
        emit -1, mem is converted to megabytes."""
        mem = -1 if self.mem is None else size_to_megs(self.mem)
        return f'cpu={self.slurm_cpus},mem={mem},gres/gpu={self.slurm_gpus}'


class SlurmAllocation(BaseDocument, Expirable):
    tres: SlurmTRES = Field(default_factory=SlurmTRES)
    comment: str = ''

    class Settings:
        name = 'slurm_allocations'


class SlurmAccountLimits(BaseModel):
    max_user_jobs: int = -1
    max_group_jobs: int = -1
    max_submit_jobs: int = -1
    max_job_length: str = '-1'


class SlurmAccount(BaseDocument):
    group: Link['Group']
    site: Link[Site]
    limits: SlurmAccountLimits = Field(default_factory=SlurmAccountLimits)
    coordinators: list[Link[User]] = Field(default_factory=list)

    class Settings:
        name = 'slurm_accounts'
        indexes = [
            IndexModel(
                [('group', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]


class SlurmPartition(BaseDocument):
    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]

    class Settings:
        name = 'slurm_partitions'
        indexes = [
            IndexModel(
                [('name', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]


class SlurmQOS(BaseDocument):
    name: Annotated[str, Field(min_length=1)]
    site: Link[Site]
    group_limits: list[Link[SlurmAllocation]] = Field(default_factory=list)
    user_limits: list[Link[SlurmAllocation]] = Field(default_factory=list)
    job_limits: list[Link[SlurmAllocation]] = Field(default_factory=list)
    priority: int = 0
    flags: list[str] = Field(default_factory=lambda: ['DenyOnLimit'])

    @field_validator('flags')
    @classmethod
    def validate_flags(cls, v):
        for flag in v:
            if flag not in SLURM_QOS_VALID_FLAGS:
                raise ValueError(f'Invalid Slurm QOS flag: {flag}')
        return v

    class Settings:
        name = 'slurm_qos'
        indexes = [
            IndexModel(
                [('name', pymongo.ASCENDING), ('site', pymongo.ASCENDING)],
                unique=True,
            ),
        ]


class SlurmAssociation(BaseDocument):
    site: Link[Site]
    account: Link[SlurmAccount]
    partition: Link[SlurmPartition]
    qos: Link[SlurmQOS]

    class Settings:
        name = 'slurm_associations'
        indexes = [
            IndexModel(
                [
                    ('site', pymongo.ASCENDING),
                    ('account', pymongo.ASCENDING),
                    ('partition', pymongo.ASCENDING),
                    ('qos', pymongo.ASCENDING),
                ],
                unique=True,
            ),
        ]
