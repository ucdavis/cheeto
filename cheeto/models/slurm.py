from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

import pymongo
from beanie import Link
from pymongo import IndexModel
from pydantic import BaseModel, Field, field_validator

from ..constants import DATA_QUOTA_REGEX, SLURM_QOS_VALID_FLAGS
from .base import BaseDocument
from .site import Site
from .user import User

if TYPE_CHECKING:
    from .group import Group


class SlurmTRES(BaseModel):
    cpus: int = -1
    gpus: int = -1
    mem: Annotated[str | None, Field(default=None, pattern=DATA_QUOTA_REGEX)]


class SlurmAllocation(BaseDocument):
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
